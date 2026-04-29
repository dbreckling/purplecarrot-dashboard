# Purple Carrot — DLVE Server-Side Event Spec

## Endpoint

```
POST https://data.dojo.phluant.com
Content-Type: application/json
```

No authentication headers required. The `script_id` in the payload identifies the source.

---

## Request Body

```json
{
  "script_id":       "nubvnAhNMDnm",
  "advertiser_id":   "1060",
  "event":           "purchase",
  "dedupe_id":       "2981445",
  "timestamp":       "2026-03-30T21:40:06Z",
  "revenue":         78.00,
  "currency":        "USD",
  "email":           "subscriber@example.com",
  "ip":              "72.14.204.99",
  "customer_id":     "2981507",
  "zip_code":        "11559"
}
```

### Field Reference

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `script_id` | string | ✅ | Provided by DLVE — identifies this as a server-side event |
| `advertiser_id` | string | ✅ | Always `"1060"` for Purple Carrot |
| `event` | string | ✅ | Always `"purchase"` |
| `dedupe_id` | string | ✅ | Your subscription/order ID (e.g. `"2981445"`) — used for deduplication |
| `timestamp` | string | ✅ | ISO 8601 UTC — time subscription was confirmed |
| `revenue` | number | ✅ | First order value in dollars |
| `currency` | string | ✅ | Always `"USD"` |
| `email` | string | ✅ | Raw email — DLVE hashes it server-side |
| `ip` | string | ✅ | Subscriber's IP from your request context — **not** your server's IP |
| `customer_id` | string | | Your internal user/customer ID |
| `zip_code` | string | | Subscriber's shipping zip |

---

## Code Examples

### Node.js
```js
await fetch('https://data.dojo.phluant.com', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    script_id:      'nubvnAhNMDnm',
    advertiser_id:  '1060',
    event:          'purchase',
    dedupe_id:      subscription.id,
    timestamp:      new Date().toISOString(),
    revenue:        subscription.firstOrderTotal,
    currency:       'USD',
    email:          user.email,
    ip:             req.ip,
    customer_id:    user.id,
    zip_code:       user.shippingZip
  })
});
```

### Python
```python
import requests

requests.post('https://data.dojo.phluant.com', json={
    'script_id':      'nubvnAhNMDnm',
    'advertiser_id':  '1060',
    'event':          'purchase',
    'dedupe_id':      subscription['id'],
    'timestamp':      datetime.utcnow().isoformat() + 'Z',
    'revenue':        subscription['first_order_total'],
    'currency':       'USD',
    'email':          user['email'],
    'ip':             request.remote_addr,
    'customer_id':    user['id'],
    'zip_code':       user['shipping_zip']
})
```

### Ruby
```ruby
require 'net/http'
require 'json'

Net::HTTP.post(
  URI('https://data.dojo.phluant.com'),
  {
    script_id:      'nubvnAhNMDnm',
    advertiser_id:  '1060',
    event:          'purchase',
    dedupe_id:      subscription[:id],
    timestamp:      Time.now.utc.iso8601,
    revenue:        subscription[:first_order_total],
    currency:       'USD',
    email:          user[:email],
    ip:             request.ip,
    customer_id:    user[:id],
    zip_code:       user[:shipping_zip]
  }.to_json,
  'Content-Type' => 'application/json'
)
```

---

## When to Fire

Fire this request **after** the subscription is confirmed in your database — not on checkout initiation. The `transaction_id` should match the order ID that appears in your internal records (the `297xxxxx` format).

## Notes

- **`ip`**: Must be the subscriber's IP captured from the incoming HTTP request, not your server's outbound IP. In most frameworks: `req.ip` (Node/Express), `request.remote_addr` (Python/Flask/Django), `request.ip` (Ruby/Rails). If routing through sGTM/Stape, pass the subscriber IP explicitly in this field — forwarded headers (X-Forwarded-For, True-Client-IP) are not read by the ingestion endpoint.
- **Deduplication**: DLVE deduplicates on `dedupe_id` — safe to retry on network failure.
- **Timing**: Fire as soon as the subscription is confirmed. No batching needed.
- **`script_id`**: DLVE will provide the final value to replace `nubvnAhNMDnm`.
