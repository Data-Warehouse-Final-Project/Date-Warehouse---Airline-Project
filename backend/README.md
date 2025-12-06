# Backend (Supabase)

This folder contains a minimal scaffold to connect to a Supabase project.

Files:
- `index.js` — example client initialization using `@supabase/supabase-js`.
- `.env.example` — example environment variables.

Setup steps:

1. Install dependencies:

```powershell
cd backend
npm install
```

2. Copy env example and set your values:

```powershell
copy .env.example .env
# then edit .env with your SUPABASE_URL and SUPABASE_KEY
```

3. Run the backend script (this just verifies env and initializes the client):

```powershell
npm start
```

Integration notes:
- If you plan to use Supabase from the frontend, prefer the anon/public key and use the Supabase client in `frontend/src`.
- For server-side operations that require elevated privileges, use a service role key on a secure server (never expose this in the frontend).
