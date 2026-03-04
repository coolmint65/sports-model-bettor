# Global Claude Instructions

These rules apply to every project.

## Root Cause

No quick fixes. Always diagnose to the root cause and devise proper solutions. Never apply patches or workarounds unless the user explicitly asks.

---

## Security & Secrets

- Never hardcode secrets or commit them to git
- Use separate API tokens/credentials for dev, staging, and prod environments
- Validate all input server-side — never trust client data
- Add rate limiting on auth and write operations

## Architecture & Code Quality

- Design architecture before building — don't let it emerge from spaghetti
- Break up large view controllers/components early
- Wrap external API calls in a clean service layer (easier to cache, swap, or extend later)
- Version database schema changes through proper migrations
- Use real feature flags, not commented-out code

## Observability

- Add crash reporting from day one
- Implement persistent logging (not just console output)
- Include a /health endpoint for every service

## Environments & Deployment

- Maintain a real staging environment that mirrors production
- Set CORS to specific origins, never *
- Set up CI/CD early — deploys come from the pipeline, not a laptop
- Document how to run, build, and deploy the project

## Testing & Resilience

- Test unhappy paths: network failures, unexpected API responses, malformed data
- Test backup restores at least once — don't wait for an emergency
- Don't assume the happy path is sufficient

## Time Handling

- Store all timestamps in UTC
- Convert to local time only on display

## Discipline

- Fix hacky code now or create a tracked ticket with a deadline — "later" never comes
- Don't skip fundamentals just because the code compiles and runs
