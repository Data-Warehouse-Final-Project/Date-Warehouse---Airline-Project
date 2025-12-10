User (Frontend) 
  ↕️ Upload file, choose CSV type, clean data locally
  ↕️ Request eligibility check

Backend (API)
  ↕️ Receives eligibility check request
  ↕️ Produces Kafka message for processing
  ↕️ Queries Supabase flight data

Supabase (DB + Realtime)
  ↕️ Stores cleaned data (from cleaning.py upserts)
  ↕️ Sends realtime eligibility results to frontend via subscription

Cleaning script
  ↕️ Runs on CSV files independently (or triggered via separate process)
  ↕️ Upserts clean data into Supabase tables
  ↕️ Saves quarantined CSV for review