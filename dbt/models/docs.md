{% docs session_cvr %}
Purchasing sessions divided by sessions. A purchasing session has at least one purchase event. Several purchase events in the same session count as one order.
{% enddocs %}

{% docs aov %}
Revenue divided by purchasing sessions. Revenue is the sum of purchase event prices. There is no quantity field on the source event.
{% enddocs %}

{% docs cart_abandonment_rate %}
Sessions with a cart event and no purchase event in the same day, category, and price band, divided by sessions with a cart event in that slice.
{% enddocs %}

{% docs retention_rate %}
Users with any event in that week, divided by users whose first event falls in the cohort week. Week 0 is the cohort week, so the rate is 1.
{% enddocs %}

{% docs activation_rate_7d %}
Share of the cohort with a first purchase at most 168 hours after the first event.
{% enddocs %}

{% docs sample_size %}
Sessions per arm for a two sided two proportion test at alpha 0.05 and power 0.8. The comparison rate is the baseline times one plus the relative MDE, capped below 1. Estimated days assume both arms run in parallel and split that metric's average daily volume.
{% enddocs %}
