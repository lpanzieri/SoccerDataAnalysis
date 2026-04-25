# Task 13 Regression Summary (2026-04-25)

## Scope
End-to-end regression validation for graphical and non-graphical helper intents with cache behavior checks.

## Test Matrix
1. Graphical goals comparison intent
- Question: graph of the goals scored by inter, milan, juventus and napoli in the last 10 years
- Expected intent: graphical_goals_comparison
- Expected contract: image true, base64_image true, meta.image_path present, meta.team_badges present

2. Best away record intent
- Question: which team has the best away record in serie a in the last 10 years
- Expected intent: best_away_record
- Expected contract: image false, rows present

3. Most points in season intent
- Question: which team has the most points in a season in serie a
- Expected intent: most_points_in_season
- Expected contract: image false, rows present

4. Most goals in season intent
- Question: which team has the most goals in a season in serie a
- Expected intent: most_goals_in_season
- Expected contract: image false, rows present

5. Longest title streak intent
- Question: which team has the most titles in a row in serie a
- Expected intent: longest_title_streak
- Expected contract: image false, rows present

## Cache Verification
- For each case, helper cache table was cleared before test.
- Then each question was run twice with cache enabled.
- Expected behavior: first run miss, second run hit.
- Result: passed for all cases.

## Result
- Regression artifact: benchmarks/task13_regression_20260425_173843.json
- Case count: 5
- Passed: 5
- Overall: PASS

## Notes
- Graphical responses are image-first payloads and do not require rows list presence.
- Contract checks for graphical cases should focus on intent, image/base64 flags, image path, and team badge metadata.
