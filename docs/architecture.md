# Architecture

```mermaid
graph LR
    webapp[Webapp] --> backend[Backend]
    bot[Bot] --> backend
    backend --> supabase[Supabase]
    backend --> redis[Redis]
    backend --> workers[Celery Workers]
    workers --> redis
    workers --> supabase
```
