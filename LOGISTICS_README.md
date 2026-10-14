# UzoAgro logistics workflow

The main `dashboard.html` now uses the shared Neon session and actual per-user records. It replaces the old timed driver-match animation and alert-only “Lock Driver” interaction. The previous dashboard source is preserved in `docs/dashboard-legacy.html`.

## Farmer journey

1. Sign in using your existing account and four-digit PIN.
2. Choose **Find a driver**, then enter pickup city, delivery city, crop, quantity in tonnes, and preferred date.
3. Compare drivers' published return trips. The existing five-factor engine ranks viable routes; capacity, timing, cargo compatibility and approximate city-level direction checks filter unsuitable trips first.
4. Select **Request pickup**. This creates a saved request in the assigned driver's workspace.
5. When the driver accepts, both parties can see the other's phone number. Follow **Accepted → In transit → Delivered** in **My shipments**.

You can cancel a request before pickup. A farmer cannot accept or update a driver's journey on their behalf. The fit percentage is a relative routing score, not a probability of successful delivery, identity verification, or quality guarantee.

## Driver journey

1. Sign in to the driver workspace.
2. Publish a return trip with city-level origin/destination, departure date, available capacity, and supported crops.
3. Review pickup requests and accept or decline them.
4. Accepting reserves the trip for one shipment and declines other pending requests on that same trip. This MVP supports one shipment per published trip.
5. Confirm pickup and delivery as those handovers happen. Completed trips stop appearing as available matches. Availability for an unreserved trip can be closed or reopened.

These are application records; the app does not send emails, SMS, external messages or payments. Participants can use the displayed phone link after acceptance to contact each other themselves. Updates refresh on page load and with the shipment **Refresh** button; there is no push notification service yet.

## Trace integration

Open a batch and choose **Find a driver for this batch** to prefill the crop and convert its kg quantity to tonnes. A saved pickup request retains the batch ID. Shipment details link to its public produce passport. Server checks enforce batch ownership and prevent a single shipment from exceeding that batch's quantity.

Transport status changes do not automatically create blockchain checkpoints. Farmers still record traceability checkpoints explicitly, so no aggregation stage or blockchain transaction is fabricated.

## Charts and data

The dashboard shows per-user request counts, active journeys, pending decisions, and tonnes delivered. The monthly chart compares requests created with deliveries completed during the past six months (or three months when selected). The status chart groups all saved requests by their current stage. Both charts have accessible labels and real zero/empty states.

Startup adds `logistics_routes`, `logistics_jobs` and an index preventing duplicate active requests to the existing database. Existing user accounts and previous logistics tables are retained. PostgreSQL row locks serialize reservations and job transitions. The API exposes route cities, not exact vehicle or farm positions; match previews do not return phone numbers. Jobs and analytics are restricted to their participants.

Geographical suitability uses approximate latitude/longitude bounds at city scale, plus the existing engine. It is not road-network optimization or a quoted transport price. Pricing, multi-stop capacity sharing, GPS tracking and automated Trace checkpoints are outside this implementation.

## Validation

```powershell
.trace-venv/Scripts/python.exe -m unittest discover -s tests -q
node --check frontend/dashboard.js
```

The account, transport and Trace tests use disposable fixtures, not real Neon users or shipments. Browser validation covers matching, requesting, accepting, contact unlocking, pickup, delivery, both dashboards and mobile rendering. `tests/browser_fixture.py` is explicitly a test-only server and is never used by `trace_server.py`.

## Local demo driver matching

Run `.trace-venv/Scripts/python.exe tests/browser_fixture.py`, then open http://127.0.0.1:8002/login.html. This server uses disposable SQLite data; it never seeds Neon. Farmer: `00000000001`; primary driver: `00000000002`; PIN: `1234`. All accounts and routes are fictional.

Try Onitsha → Lagos, Cassava, 0.25 tonnes, today: Chidi and Ngozi qualify. The small-capacity and reverse-direction drivers do not. Abuja → Kano with Rice and Lagos → Ibadan with Tomatoes exercise other sample routes. Drivers can inspect seeded farmer pickup requests and accept, pick up, and deliver shipments. Restarting the server resets records and refreshes sample route dates.
