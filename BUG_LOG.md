# Bug log

## B1: lchttp.Transport response body unreadable when PerTryTimeout is set
- Found by: TestTransportBodyReadable (lchttp), with a backend that flushes headers before the body
- Symptom: with ClientConfig.PerTryTimeout > 0, reading the body of the response returned by Transport.RoundTrip failed with "context canceled" whenever the body had not fully arrived with the headers.
- Cause: Client.Do cancels each attempt's per-try context as soon as the attempt function returns, but the HTTP request was sent on that context, so the cancel tore down the response stream before the caller could read it.
- Fix: Transport sends each attempt on its own context derived from the caller's context with the same deadline as the per-try context, and cancels it when the response body is closed (or the round trip fails).
