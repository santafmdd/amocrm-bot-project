# Weekly control, manager analysis, weekly planning and base analysis - target business spec

## 1. Purpose

Service purpose:

1. Pull factual data from amoCRM on deals, calls, stages, notes, tasks, tags, contacts and companies.
2. Build a day-by-day control layer that looks like normal management control, not like one big weekly dump.
3. Build a weekly summary separately for each manager.
4. Build one weekly summary for the Monday team meeting.
5. Build my planned activities for the next week by days.
6. Build a separate strategic analysis of bases/segments for market research and workload distribution.
7. Use Google Sheets as the current write target, but keep architecture storage-agnostic so that later the writer can be replaced with a Yandex-based writer without rewriting business logic.

Important: all business texts must not look like AI-generated text.  
The assistant must avoid:
- dead official wording,
- template phrases,
- symmetrical polished wording,
- generic “important to note / it is observed / attention should be paid” phrasing,
- obvious LLM tone.

Texts must look like working management notes.

---

## 2. Main data sources

### 2.1 amoCRM
amoCRM is the main factual source for:
- deals,
- companies,
- contacts,
- tags,
- calls,
- notes,
- tasks,
- stage movements,
- responsible managers,
- products,
- fields like brief, demo result, test result, comments, etc.

### 2.2 Main management workbook
Google Sheets workbook:

`https://docs.google.com/spreadsheets/d/1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0/edit?gid=1057004550#gid=1057004550`

This is the workbook "РОКС 2026".

This workbook is used as:
- current reporting destination,
- source of monthly and weekly plan/fact context,
- source for weekly meeting context,
- source for base whitelist settings,
- future destination for manager control layers and strategic base analysis.

### 2.3 Client list
Google Sheets workbook:

`https://docs.google.com/spreadsheets/d/1Mvumt1o0z_9325rHkf8O7bCjk4-Qv7c5l4uAiRgvYsc/edit?gid=694650030#gid=694650030`

Used as enrich source for:
- test start,
- test completion,
- client progress fields,
- other useful client-side process markers.

### 2.4 Appointment table
Google Sheets workbook:

`https://docs.google.com/spreadsheets/d/1nZz_DL2Io0EII7Qt4elVIdpKrU5Kzc1cbnQ9FEmT0EY/edit?pli=1&gid=267524536#gid=267524536`

Important:
- this workbook has multiple year tabs,
- the active tab for current logic is the tab for year `2026`.

Used as enrich source for:
- scheduled appointments,
- appointment outcomes,
- appointment confirmations,
- additional context for meeting quality and attendance discipline.

---

## 3. General business logic

### 3.1 Main principle
The service is not a one-time weekly report generator.

It must build several layers:

1. Daily control layer.
2. Weekly summary for each manager.
3. Weekly meeting summary for me / my leader.
4. Next-week action plan by days.
5. Separate strategic base analysis layer.

### 3.2 Deal is not “analyzed once forever”
One deal can and must be taken into analysis again if a new episode appears.

A new episode is any of the following:
- a new call appeared after the last analyzed call,
- the deal moved to another stage,
- new meaningful information appeared in the deal,
- a new tag/base appeared,
- a new brief / demo result / test result / follow-up block appeared,
- the business context materially changed.

Therefore the service must think in terms of:
- `deal_id`
- plus `analysis episode`

and not just “deal already analyzed => ignore forever”.

### 3.3 Primary factual anchor for daily control
The main anchor is the **date and time of the last call**.

Calls are the primary basis for analysis.  
Other CRM data is additional context and enrich.

---

## 4. Manager role profiles

The service must not analyze all managers by the same logic.

### 4.1 Rustam profile
Role profile: outbound / cold stage.

Main focus:
- cold outbound,
- getting to the decision maker,
- qualification,
- need discovery,
- moving to interest,
- appointment setting.

Typical key zone:
- from early statuses up to "есть интерес" / meeting assignment.

### 4.2 Ilya profile
Role profile: warm/inbound + demo/test/follow-up.

Main focus:
- handling incoming or warmer contacts,
- moving from interest to demo,
- confirming demo,
- conducting demo,
- opening test,
- follow-up,
- moving to invoice / payment.

Typical key zone:
- from "есть интерес" / "проведена демонстрация" and further.

Important: weekly and daily analysis for Rustam and Ilya must use different evaluation accents and different prioritization of growth areas.

---

## 5. Daily control layer

## 5.1 Why daily layer is needed
It must look like normal operational control.  
A single weekly block is not enough and looks unnatural.

At the same time the service may be run on weekends.  
Therefore daily control must be built as **retro control packages**, not as a claim that the system was literally launched every day.

Correct interpretation:
- “control package for Monday”
- “control package for Tuesday”
- etc.

Not:
- “I literally sat on Monday and analyzed it live”.

## 5.2 Daily control grain
One row = one manager + one control day + one control package.

Example:
- week 2026-04-13..2026-04-18
- manager = Rustam
- control_day = Monday
- control package = package for Monday

## 5.3 Daily package causal rule
Daily package must not use future information.

For a given control day, only calls/deal state that were already known by the cut-off of that day may be used.

### Cut-off rule
The day cut-off is `14:00`.

Example logic:
- Monday package: calls and deal state known up to Monday 14:00, plus recent carryover from previous week if there is not enough material.
- Tuesday package: new material after Monday 14:00 and up to Tuesday 14:00, plus nearest not-yet-used recent carryover if needed.
- Wednesday package: new material after Tuesday 14:00 and up to Wednesday 14:00.
- etc.

Important:
- Monday package must not include calls from Tuesday or Wednesday.
- Tuesday package must not include data that only became available after Tuesday 14:00.
- No future leakage is allowed.

## 5.4 Number of control days
Default:
- Monday-Friday.

Saturday:
- add only if there is real noticeable Saturday CRM activity.

Holiday / zero-activity day:
- if there is truly no material, the day is considered inactive,
- if there is little but still relevant material, allow a thin package.

## 5.5 Package size
Weekly pool per manager:
- target up to 30 deals per analyzed week.

Daily package target:
- around 5-6 deals on average.

But:
- if it is impossible to gather enough fresh material without going too far back in time,
- then package may be thin: 1-3 deals.

Important:
- do not dig too deep into old history just to force a fake volume.
- recent and honest thin package is better than an inflated irrelevant package.

## 5.6 Carryover rule
If a day lacks fresh material, service may use carryover:
- from current analyzed week,
- from immediately previous week,
- but not from deep old periods.

Deep old deals must not be used just to fill the row.

## 5.7 Daily control output fields
Daily control row should contain at least:

- `week_start`
- `week_end`
- `control_day_date`
- `control_day_label`
- `manager_name`
- `manager_role_profile`
- `sample_size`
- `deal_ids`
- `deal_links`
- `product_mix`
- `base_mix`
- `main_pattern`
- `strong_sides`
- `growth_zones`
- `why_it_matters`
- `reinforcement_actions`
- `correction_actions`
- `daily_standup_message`
- `expected_quant_impact`
- `expected_qual_impact`
- `highlight_case`
- `analysis_backend_used`

Important:
- `product_mix` = distribution of INFO / LINK in this control package.
- `base_mix` = which bases/segments dominate the package.

---

## 6. Weekly manager summary

## 6.1 Grain
One row = one manager + one week.

Separate rows:
- Ilya weekly summary,
- Rustam weekly summary.

No combined deep analysis row for both managers together.

## 6.2 Weekly manager summary purpose
This row is the accumulated weekly management summary for a specific manager.

It must answer:
- what happened this week,
- what improved,
- what did not improve,
- what repeated,
- what training is needed,
- what tasks should be given next week,
- what quantitative and qualitative effect is expected.

## 6.3 Weekly manager summary fields
Suggested minimum fields:

- `week_start`
- `week_end`
- `manager_name`
- `manager_role_profile`
- `deals_analyzed_total`
- `product_mix_week`
- `base_mix_week`
- `week_summary`
- `what_improved`
- `what_not_improved`
- `repeat_problems`
- `strong_sides_week`
- `growth_zones_week`
- `employee_training_summary`
- `employee_fix_tasks`
- `manager_next_week_actions`
- `expected_quant_impact`
- `expected_qual_impact`
- `director_safe_summary`
- `employee_message_style_output`

---

## 7. Next-week action plan

## 7.1 Why a separate layer is needed
Weekly summary is retrospective.  
Plan layer is forward-looking.  
They must not be mixed into one messy block.

## 7.2 Grain
One row = one day of next week + one target.

Target can be:
- Ilya,
- Rustam,
- team/department.

## 7.3 Plan logic
The service must not simply list all issues.  
It must prioritize.

Priority order:
1. what is economically more useful to fix first,
2. what is easier and faster to implement,
3. what reinforces already existing strengths,
4. what can realistically be split into logical steps across the week.

This means:
- simple actions may be split into several sequential steps,
- but only if that split is meaningful and believable,
- not just for fake busyness.

## 7.4 Plan row fields
Suggested minimum fields:

- `plan_week_start`
- `plan_week_end`
- `plan_day_date`
- `plan_day_label`
- `target_type`
- `target_name`
- `priority`
- `activity_type`
- `what_i_do`
- `what_i_say_on_daily`
- `what_task_i_give`
- `what_i_check`
- `expected_quant_impact`
- `expected_qual_impact`
- `link_to_source_weekly_findings`

Activity types:
- operational,
- control,
- coaching,
- development,
- strategic support.

---

## 8. Weekly meeting summary

## 8.1 Purpose
This is the Monday meeting speech layer.

It must combine:
- previous week results,
- planned next week actions,
- weekly manager analysis,
- monthly context from ROKS,
- short strategic accents.

## 8.2 Grain
One row = one week.

## 8.3 What this layer must contain
Suggested fields:

- `week_start`
- `week_end`
- `previous_week_summary`
- `team_week_summary`
- `what_changed_quantitatively`
- `what_changed_qualitatively`
- `what_did_not_work`
- `next_week_focus`
- `next_week_plan_summary`
- `weekly_meeting_brief`
- `strategic_accents`
- `risk_accents`
- `director_safe_talking_points`

Important:
This layer must sound like a competent weekly management speech, not like an LLM memo.

It must include not only operational points, but also 1-3 broader accents:
- funnel bottleneck,
- overload by product,
- market/base quality issue,
- focus shift for next week,
- quality vs quantity tradeoff,
- etc.

---

## 9. ROKS usage logic

## 9.1 What ROKS contains
ROKS contains:
- monthly plan/fact for department,
- monthly plan/fact for each manager,
- weekly plan/fact blocks,
- right-side block with:
  - `%` completion,
  - `forecast`,
  - `residual`,
  - monthly conversions.

## 9.2 How this data must be used
ROKS is not just a visual table.  
It is a factual planning context for:
- weekly meeting brief,
- next-week plan,
- strategic accents,
- base workload decisions.

## 9.3 Important ROKS semantics
If there is a manager surname to the left:
- values belong to that manager.

If there is "отдел":
- values belong to the whole department.

Weekly columns are:
- week number,
- plan,
- fact.

Monthly block is:
- month plan,
- month fact.

Right-side block:
- `%` = monthly completion percentage fact/plan,
- `forecast` = trend forecast if current pace stays similar,
- `residual` = plan minus fact.

## 9.4 Conversion calculation rules
Monthly conversion block must be interpreted carefully.

Correct idea:
- take funnel rows,
- lower stage divided by upper stage,
- multiply by 100%.

Important:
do not calculate nonsense conversions like:
- rubles to pieces,
- payment rubles to payment count,
- payment count to invoice rubles.

These may be useful for average check understanding, but not as funnel conversion.

The service must only use meaningful funnel pairs.

---

## 10. Base / segment analysis

## 10.1 Why it is a separate layer
This is a separate strategic task:
- market research,
- base quality evaluation,
- decision whether to continue or stop the segment,
- future workload distribution.

Therefore this must have its own sheet and its own logic.

## 10.2 Source of segments
Segments are defined by tags.

Priority:
1. use deal tag,
2. if deal tag is absent, fallback to company tag.

Important:
- the service must preserve all matched tags,
- but it must also define a primary working segment for aggregation.

## 10.3 Whitelist source
The whitelist of bases/tags must be maintained in a sheet inside the main workbook.

Recommended sheet name:
- `база`

Minimal column:
- `база`

Possible future useful columns:
- `активно`
- `приоритет`
- `комментарий`
- `продукт`
- `цель теста`

For current logic, the service must analyze only tags explicitly listed in this sheet.  
It must not drag every random CRM tag into segment analysis.

## 10.4 Current known base tags
Current working tags:

- `стройка_от100млн_линк`
- `легкая_промышленность_от50млн_инфо`
- `машиностроение`

Important business context:
- `стройка_от100млн_линк` was actively tested in February-March and may still appear as a “late tail”.
- `легкая_промышленность_от50млн_инфо` is currently actively tested on two products.
- `машиностроение` is currently actively tested on two products.

## 10.5 Analysis periods for bases
For each active base/segment the service must build two mandatory views:
1. full historical processed period (`all_time`)
2. recent view (`last_30_days`)

This is needed because:
- some segments may look good historically but weaken recently,
- some may look weak historically but show recent traction,
- strategic decisions require both long and short horizon.

## 10.6 Base analysis grain
One row = one base + one period + optionally one product scope.

At minimum:
- base_tag
- analysis_period_type
- product_scope

## 10.7 Base evaluation language
Business-friendly evaluation labels:

- `пиздато`
- `норм`
- `терпимо`
- `сойдет`
- `хуйня`

Internally logic may normalize them, but exports must use these business labels.

## 10.8 Base analysis fields
Suggested minimum fields:

- `analysis_period_type`
- `period_start`
- `period_end`
- `base_tag`
- `all_detected_tags`
- `primary_tag`
- `tag_source_mode`
- `product_scope`
- `deals_total`
- `calls_total`
- `appointments_total`
- `demos_total`
- `tests_total`
- `invoices_total`
- `payments_total`
- `main_conversions`
- `main_refusal_patterns`
- `strong_patterns`
- `weak_patterns`
- `market_fit_comment`
- `base_grade`
- `base_grade_reason`
- `take_into_work_decision`
- `take_into_work_reason`
- `fixable_or_not`
- `what_to_change`
- `what_to_ignore`
- `recommended_total_load_percent`
- `recommended_info_load_percent`
- `recommended_link_load_percent`
- `recommended_owner_profile`
- `next_period_strategy`

## 10.9 Mandatory strategic decision outputs
For each base the service must answer:
- take into work,
- do not take into work,
- take into work with conditions.

Also:
- why,
- what can be fixed,
- what is not worth fixing,
- what share of future workload should be allocated,
- for which product,
- for which manager profile.

## 10.10 Additional subsegment recommendation
For each base the service must also produce recommendations for further narrowing / subsegmenting.

This includes, when possible:
- activity type,
- recommended OKVED list,
- balance range,
- region,
- any other practical filtering dimension that makes sense.

This recommendation must answer:
- how to sharpen the base,
- what additional filter to add,
- whether broader base is bad but a narrower pocket inside it may still be good.

This block is mandatory because market research is one of the defended strategic tasks.

---

## 11. Enrichment logic

The analyzer must enrich CRM analysis with external sheets:

### 11.1 Client list enrich
Use client list to pull:
- test started,
- test finished,
- useful client process statuses,
- additional context for whether the deal is really progressing.

### 11.2 Appointment table enrich
Use appointment table for year `2026` tab to pull:
- meeting assignment,
- confirmation,
- attendance / held vs not held,
- useful meeting-level context.

### 11.3 Matching priority
Suggested matching priority:
1. deal_id
2. phone
3. email
4. company + contact
5. company only

The output must clearly show:
- whether enrich matched,
- from which source,
- confidence level.

---

## 12. Style rules for generated texts

This is mandatory.

### 12.1 Employee-facing style
For employee messages:
- more direct,
- simpler,
- closer to my lexicon,
- no dead office language,
- more concrete action wording.

### 12.2 My internal working style
For my internal notes:
- dry,
- practical,
- structured,
- no decorative text.

### 12.3 Leader-safe style
For my leader:
- calm,
- business-like,
- not too soft,
- not obviously AI,
- no robotic or polished over-explanation.

### 12.4 Anti-AI requirements
Avoid:
- identical sentence length everywhere,
- generic intro phrases,
- over-balanced three-part lists,
- “this indicates / it should be noted / important to note / it is observed that” phrasing,
- over-clean grammar that sounds machine-made.

Output must feel like human management notes.

---

## 13. Storage / writer architecture

Current writer target:
- Google Sheets.

Future requirement:
- possible migration from Google-based storage to Yandex-based storage.

Therefore:
- business logic must be independent of storage vendor,
- writer must be adapter-based,
- source logic and analysis logic must not be tightly coupled to Google APIs,
- current Google writer is only one implementation of a storage adapter.

This applies not only to the future analyzer writer, but also later to weekly analytics blocks and refusal blocks.

---

## 14. What is considered “done” for this analyzer system

The analyzer system can be considered working only when all of the following exist:

1. Daily control packages by manager.
2. Weekly manager summary for Ilya and Rustam separately.
3. Weekly meeting summary.
4. Next-week plan by days.
5. Separate base analysis sheet.
6. Base decisions:
   - take / do not take / take with condition.
7. Base workload distribution recommendations.
8. Enrich from client list and appointment table.
9. Re-analysis of deals by episodes, not one-time static analysis.
10. Text outputs that do not smell like AI.

---

## 15. Current business shorthand summary

In simple business words, the service must do this:

- collect real factual CRM material,
- enrich it with client list and appointments,
- analyze calls and deal episodes,
- split weekly control into believable daily control packages,
- separately summarize Ilya and Rustam for the week,
- prepare what I will say on the Monday weekly meeting,
- prepare what I will do and say next week by days,
- separately analyze chosen bases/segments historically and for the last 30 days,
- recommend whether to continue, drop or conditionally continue those bases,
- recommend how to distribute future effort by segment, product and manager profile,
- and write all that into the working workbook in a way that looks like a normal management system, not an AI artifact.