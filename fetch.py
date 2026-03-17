import requests
import json
import os
from datetime import datetime, timedelta, timezone

TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO = "PostHog/posthog"
BASE = "https://api.github.com"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

CUTOFF = datetime.now(timezone.utc) - timedelta(days=90)

def get_prs():
    prs = []
    page = 1
    print("Fetching PRs...")
    while True:
        url = f"{BASE}/repos/{REPO}/pulls"
        resp = requests.get(url, headers=HEADERS, params={
            "state": "closed",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
            "page": page
        })
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for pr in batch:
            if not pr.get("merged_at"):
                continue
            merged_at = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
            if merged_at < CUTOFF:
                print(f"  Reached cutoff at page {page}. Done.")
                return prs
            prs.append(pr)
        print(f"  Page {page}: {len(batch)} PRs, total merged so far: {len(prs)}")
        page += 1
    return prs

def get_reviews(pr_number):
    url = f"{BASE}/repos/{REPO}/pulls/{pr_number}/reviews"
    resp = requests.get(url, headers=HEADERS, params={"per_page": 100})
    if resp.status_code != 200:
        return []
    return resp.json()

def get_review_comments(pr_number):
    url = f"{BASE}/repos/{REPO}/pulls/{pr_number}/comments"
    resp = requests.get(url, headers=HEADERS, params={"per_page": 100})
    if resp.status_code != 200:
        return []
    return resp.json()

def get_files(pr_number):
    url = f"{BASE}/repos/{REPO}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=HEADERS, params={"per_page": 100})
    if resp.status_code != 200:
        return []
    return resp.json()

BOT_KEYWORDS = ["[bot]", "bot", "copilot", "dependabot", "graphite", "greptile", "renovate", "codecov"]

def is_bot(login):
    login_lower = login.lower()
    return any(k in login_lower for k in BOT_KEYWORDS) or login.endswith("[bot]")

def main():
    prs = get_prs()
    if not prs:
        print("No PRs found.")
        return

    print(f"\nTotal merged PRs in last 90 days: {len(prs)}")
    print("Fetching review and file data for each PR...")

    authors = {}

    for i, pr in enumerate(prs):
        author = pr["user"]["login"]
        if is_bot(author):
            continue
        number = pr["number"]
        created = datetime.fromisoformat(pr["created_at"].replace("Z", "+00:00"))
        merged = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
        days_open = (merged - created).total_seconds() / 86400

        if author not in authors:
            authors[author] = {
                "login": author,
                "merged_prs": 0,
                "total_days_open": 0,
                "meaningful_reviews_given": 0,
                "review_threads_started": 0,
                "dirs_touched": set(),
                "pr_numbers": []
            }

        a = authors[author]
        a["merged_prs"] += 1
        a["total_days_open"] += days_open
        a["pr_numbers"].append(number)

        # Get files changed in this PR
        files = get_files(number)
        for f in files:
            path = f.get("filename", "")
            parts = path.split("/")
            if len(parts) > 1:
                a["dirs_touched"].add(parts[0] + "/" + parts[1])
            else:
                a["dirs_touched"].add(parts[0])

        if (i + 1) % 10 == 0:
            print(f"  Processed {i+1}/{len(prs)} PRs...")

    # Now fetch reviews — what did each person review (not author)
    print("\nFetching review comments left by engineers...")
    for i, pr in enumerate(prs):
        pr_author = pr["user"]["login"]
        number = pr["number"]

        reviews = get_reviews(number)
        for rev in reviews:
            reviewer = rev["user"]["login"] if rev.get("user") else None
            if not reviewer or reviewer == pr_author or is_bot(reviewer):
                continue
            body = rev.get("body", "") or ""
            if reviewer not in authors:
                authors[reviewer] = {
                    "login": reviewer,
                    "merged_prs": 0,
                    "total_days_open": 0,
                    "meaningful_reviews_given": 0,
                    "review_threads_started": 0,
                    "dirs_touched": set(),
                    "pr_numbers": []
                }
            if len(body) > 50:
                authors[reviewer]["meaningful_reviews_given"] += 1

        inline = get_review_comments(number)
        threads_started = set()
        for c in inline:
            reviewer = c["user"]["login"] if c.get("user") else None
            if not reviewer or reviewer == pr_author or is_bot(reviewer):
                continue
            if reviewer not in authors:
                authors[reviewer] = {
                    "login": reviewer,
                    "merged_prs": 0,
                    "total_days_open": 0,
                    "meaningful_reviews_given": 0,
                    "review_threads_started": 0,
                    "dirs_touched": set(),
                    "pr_numbers": []
                }
            body = c.get("body", "") or ""
            if len(body) > 50:
                authors[reviewer]["meaningful_reviews_given"] += 1
            # Count unique threads per reviewer per PR
            thread_key = (reviewer, c.get("pull_request_review_id"))
            if thread_key not in threads_started:
                threads_started.add(thread_key)
                authors[reviewer]["review_threads_started"] += 1

        if (i + 1) % 10 == 0:
            print(f"  Reviews processed: {i+1}/{len(prs)} PRs...")

    # Serialize sets to lists and compute avg_days_open + speed_bonus
    for a in authors.values():
        a["dirs_touched"] = list(a["dirs_touched"])
        a["unique_dirs"] = len(a["dirs_touched"])
        if a["merged_prs"] > 0:
            a["avg_days_open"] = round(a["total_days_open"] / a["merged_prs"], 2)
        else:
            a["avg_days_open"] = 0
        # Speed bonus: inverse of avg days open (capped so 0-day PRs don't explode)
        a["speed_bonus"] = round(1 / (a["avg_days_open"] + 0.5), 3) if a["merged_prs"] > 0 else 0

    # Compute team averages (only among real humans who merged at least 1 PR)
    active = [a for a in authors.values() if a["merged_prs"] >= 1 and not is_bot(a["login"])]
    def avg(key):
        vals = [a[key] for a in active]
        return sum(vals) / len(vals) if vals else 1

    avg_merged       = avg("merged_prs")
    avg_reviews      = avg("meaningful_reviews_given") or 1
    avg_threads      = avg("review_threads_started") or 1
    avg_dirs         = avg("unique_dirs") or 1
    avg_speed        = avg("speed_bonus") or 1

    print(f"\nTeam averages → merged: {avg_merged:.1f}, reviews: {avg_reviews:.1f}, threads: {avg_threads:.1f}, dirs: {avg_dirs:.1f}, speed: {avg_speed:.3f}")

    WEIGHTS = {
        "merged_prs":   2.0,
        "reviews":      1.5,
        "threads":      1.0,
        "dirs":         1.5,
        "speed":        1.0,
    }

    for a in authors.values():
        norm_merged  = a["merged_prs"] / avg_merged
        norm_reviews = a["meaningful_reviews_given"] / avg_reviews
        norm_threads = a["review_threads_started"] / avg_threads
        norm_dirs    = a["unique_dirs"] / avg_dirs
        norm_speed   = a["speed_bonus"] / avg_speed

        score = (
            norm_merged  * WEIGHTS["merged_prs"] +
            norm_reviews * WEIGHTS["reviews"] +
            norm_threads * WEIGHTS["threads"] +
            norm_dirs    * WEIGHTS["dirs"] +
            norm_speed   * WEIGHTS["speed"]
        )
        a["impact_score"] = round(score, 3)
        a["normalized"] = {
            "merged_prs":   round(norm_merged, 3),
            "reviews":      round(norm_reviews, 3),
            "threads":      round(norm_threads, 3),
            "dirs":         round(norm_dirs, 3),
            "speed":        round(norm_speed, 3),
        }

    # Sort and pick top 5 (human engineers only)
    ranked = sorted(
        [a for a in authors.values() if not is_bot(a["login"])],
        key=lambda x: x["impact_score"],
        reverse=True
    )
    top5 = ranked[:5]

    # Assign persona tags
    def assign_persona(eng, all_eng):
        # Find who has max in each category
        max_merge_rate = max(all_eng, key=lambda x: x["merged_prs"])
        max_review_ratio = max(all_eng, key=lambda x: (
            x["meaningful_reviews_given"] / (x["merged_prs"] + 1)
        ))
        max_specialist = max(all_eng, key=lambda x: x["unique_dirs"])
        max_speed = max(all_eng, key=lambda x: x["speed_bonus"])

        if eng["login"] == max_review_ratio["login"]:
            return {"tag": "The Shield", "emoji": "🛡️", "reason": "Highest review-to-author ratio"}
        elif eng["login"] == max_merge_rate["login"]:
            return {"tag": "The Closer", "emoji": "🚀", "reason": "Highest PR merge volume"}
        elif eng["login"] == max_speed["login"]:
            return {"tag": "The Accelerator", "emoji": "⚡", "reason": "Fastest average PR turnaround"}
        elif eng["login"] == max_specialist["login"]:
            return {"tag": "The Specialist", "emoji": "🔬", "reason": "Widest cross-area breadth"}
        else:
            return {"tag": "The Builder", "emoji": "🏗️", "reason": "Strong all-round contributor"}

    for eng in top5:
        eng["persona"] = assign_persona(eng, top5)
        del eng["pr_numbers"]  # keep data.json small

    # Calculate date coverage
    oldest_pr_date = None
    for pr in prs:
        merged = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
        if oldest_pr_date is None or merged < oldest_pr_date:
            oldest_pr_date = merged

    days_covered = (datetime.now(timezone.utc) - oldest_pr_date).days if oldest_pr_date else 90

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days_covered": days_covered,
        "total_prs_analyzed": len(prs),
        "cutoff_date": CUTOFF.isoformat(),
        "weights": WEIGHTS,
        "team_averages": {
            "merged_prs": round(avg_merged, 2),
            "meaningful_reviews": round(avg_reviews, 2),
            "review_threads": round(avg_threads, 2),
            "unique_dirs": round(avg_dirs, 2),
            "speed_bonus": round(avg_speed, 3),
        },
        "top5": top5
    }

    with open("data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Done! data.json written.")
    print(f"   Days covered: {days_covered}")
    print(f"   PRs analyzed: {len(prs)}")
    print(f"\nTop 5 Engineers:")
    for i, eng in enumerate(top5, 1):
        print(f"  {i}. {eng['login']} ({eng['persona']['tag']}) — score: {eng['impact_score']}")

if __name__ == "__main__":
    main()
