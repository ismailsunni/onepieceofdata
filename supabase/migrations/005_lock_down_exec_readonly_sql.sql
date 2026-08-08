-- Migration 005: lock down exec_readonly_sql + profiles RLS
--
-- Fixes two issues reported externally (2026-08-08):
--   1. exec_readonly_sql (003) is SECURITY DEFINER with no REVOKE, so the
--      default PUBLIC EXECUTE grant left it callable with the anon key. Its
--      only guard was a keyword blocklist, which `select * from auth.users`
--      passes -- running as the function owner and bypassing RLS.
--   2. profiles (004) had `FOR SELECT USING (true)`, exposing every signed-in
--      user's Google email to anyone holding the anon key.

-- ---------------------------------------------------------------------------
-- 1. Restricted role the RAG queries actually execute as.
--    No USAGE on the auth schema, SELECT only on public content tables.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rag_readonly') THEN
    CREATE ROLE rag_readonly NOLOGIN NOINHERIT;
  END IF;
  -- the function owner must be a member to SET ROLE into it
  EXECUTE format('GRANT rag_readonly TO %I', current_user);
END
$$;

REVOKE ALL ON SCHEMA public FROM rag_readonly;
GRANT USAGE ON SCHEMA public TO rag_readonly;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM rag_readonly;
GRANT SELECT ON
  public.arc,
  public.chapter,
  public.character,
  public.character_affiliation,
  public.character_devil_fruit,
  public.character_occupation,
  public.graph_edges,
  public.graph_nodes,
  public.saga,
  public.volume,
  public.wiki_chunks,
  public.wiki_text
TO rag_readonly;
-- deliberately NOT granted: public.profiles, anything in auth/storage/vault

-- ---------------------------------------------------------------------------
-- 2. Rewrite the function: enforcement moves from string matching to the
--    engine. The old keyword blocklist is dropped -- it also rejected
--    legitimate queries, since e.g. `created_at` matches `create`.
--
--    Containment comes from OWNERSHIP, not SET ROLE: a SECURITY DEFINER
--    function executes as its owner, and Postgres rejects `SET ROLE` inside
--    one ("cannot set parameter role within security-definer function").
--    So the function is owned by rag_readonly and inherits exactly its
--    privileges -- no auth schema, no profiles, and RLS still applies.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.exec_readonly_sql(query_text TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    result JSONB;
BEGIN
    IF lower(trim(query_text)) NOT LIKE 'select%' THEN
        RAISE EXCEPTION 'Only SELECT queries are allowed';
    END IF;

    IF strpos(rtrim(query_text, '; '), ';') > 0 THEN
        RAISE EXCEPTION 'Multiple statements are not allowed';
    END IF;

    SET LOCAL transaction_read_only = on;
    SET LOCAL statement_timeout = '5s';

    EXECUTE format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (SELECT * FROM (%s) q LIMIT 200) t',
        rtrim(query_text, '; ')
    ) INTO result;

    RETURN COALESCE(result, '[]'::jsonb);
END;
$$;

-- an object's new owner needs CREATE on the schema; grant it only for the
-- duration of the ownership change, then take it back.
GRANT CREATE ON SCHEMA public TO rag_readonly;
ALTER FUNCTION public.exec_readonly_sql(text) OWNER TO rag_readonly;
REVOKE CREATE ON SCHEMA public FROM rag_readonly;

-- ---------------------------------------------------------------------------
-- 3. Take EXECUTE away from PUBLIC. The only caller (supabase/functions/chat
--    in the onepieceofdata-react repo) uses the service-role key.
-- ---------------------------------------------------------------------------
REVOKE ALL ON FUNCTION public.exec_readonly_sql(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.exec_readonly_sql(text) FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION public.exec_readonly_sql(text) TO service_role;

-- ---------------------------------------------------------------------------
-- 4. profiles: readable only by their owner (matches the existing UPDATE
--    policy). Both callers already filter on `.eq('id', user.id)`.
-- ---------------------------------------------------------------------------
DROP POLICY IF EXISTS "Public profiles readable" ON public.profiles;
DROP POLICY IF EXISTS "Users read own profile" ON public.profiles;
CREATE POLICY "Users read own profile" ON public.profiles
  FOR SELECT USING (auth.uid() = id);
