# Tableau Public workbook

Four views, one workbook. Connect Tableau Public to the mart CSV files in `data/exports/` after `make run` or `make export`.

This page is the spec. It is not a published dashboard, and it does not include a screenshot. After you publish, replace `tableau_public_url` in `dbt/dbt_project.yml`. Until then the dbt exposure points here:

https://github.com/Rutvij1998/clickstream-to-cohorts/blob/main/dashboards/README.md

Do not type metric values into captions. Let the sheets read the marts. If a number is not in the extract, leave it out.

## 1. Funnel health

Sources: `mart_daily_kpis`, `mart_funnel_daily_by_category`.

| Field | Sheet use |
| --- | --- |
| `activity_date` | Date axis |
| `sessions`, `purchasing_sessions`, `revenue` | Daily lines or bars |
| `cvr`, `aov` | Dual axis on the same date |
| `event_date`, `category_l1` | Funnel small multiples |
| `view_sessions`, `cart_sessions`, `purchase_sessions` | Three step bar |
| `view_to_cart_rate`, `cart_to_purchase_rate`, `session_cvr` | Rate labels |
| `cart_without_view_sessions`, `purchase_without_cart_sessions` | Exception callout, not dropped from the funnel |

`session_cvr` on the category mart is purchase sessions divided by view sessions, so it can differ from the daily KPI when a session has no view. Keep both, and title them differently.

## 2. Retention and activation

Sources: `mart_cohort_retention_weekly`, `mart_activation`.

| Field | Sheet use |
| --- | --- |
| `cohort_week` | Rows of the heatmap, and the bar axis |
| `weeks_since` | Columns of the heatmap |
| `cohort_size`, `active_users` | Detail tooltip |
| `retention_rate` | Heatmap color. Week 0 is 1 by test |
| `activated_users_7d`, `activation_rate_7d` | Bar of 7 day activation |
| `median_hours_to_first_purchase` | Tooltip or a second bar. Null when the cohort has no purchaser |

Activation is a first purchase at most 168 hours after the first event.

## 3. Revenue drivers

Sources: `mart_revenue_drivers_daily`, `mart_category_monetization`, `mart_cart_abandonment`.

| Field | Sheet use |
| --- | --- |
| `activity_date` | Date axis |
| `sessions`, `cvr`, `aov`, `revenue` | Current day |
| `sessions_previous_observed_day`, `cvr_previous_observed_day`, `aov_previous_observed_day`, `revenue_previous_observed_day` | Prior observed day, not a filled calendar day |
| `category_l1`, `view_events`, `revenue` | Category table |
| `view_share`, `revenue_share` | Scatter, one point per category |
| `revenue_per_view_share` | Tooltip. Revenue share divided by view share |
| `price_band` | Filter: `under_10`, `10_to_50`, `50_to_200`, `200_to_500`, `500_plus` |
| `cart_sessions`, `abandoned_sessions`, `cart_abandonment_rate` | Abandonment by day, category, and price band |

`Unknown` is a real category level. Do not filter it out by default.

## 4. Experiment planner

Source: `mart_experiment_baselines`.

| Field | Sheet use |
| --- | --- |
| `metric_name` | `session_cvr`, `view_to_cart`, or `cart_to_purchase` |
| `mde_relative` | 0.02 or 0.05 |
| `baseline_rate`, `baseline_variance`, `mde_absolute`, `comparison_rate` | Inputs shown as a table, not retyped |
| `alpha`, `power`, `z_alpha`, `z_beta` | Fixed design: 0.05, 0.80, and the matching z values |
| `daily_volume` | Average daily sessions for that metric |
| `required_sample_size_per_arm` | Headline number |
| `estimated_days_to_run` | Both arms in parallel: ceil of `2 * sample size / daily volume` |

One row per metric and MDE. No calculated field should reimplement the sample size formula. The mart already did.

## Publish checklist

1. Export the eight marts with `make export`.
2. Build the four sheets in Tableau Public from those extracts.
3. Publish the workbook.
4. Set `tableau_public_url` in `dbt/dbt_project.yml` to that URL.
5. Replace the placeholder link in the root README.
