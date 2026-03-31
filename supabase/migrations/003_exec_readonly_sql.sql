-- Read-only SQL execution function for RAG chatbot
-- Only allows SELECT statements
CREATE OR REPLACE FUNCTION exec_readonly_sql(query_text TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    result JSONB;
BEGIN
    -- Only allow SELECT
    IF NOT (lower(trim(query_text)) LIKE 'select%') THEN
        RAISE EXCEPTION 'Only SELECT queries are allowed';
    END IF;
    
    -- Block dangerous patterns
    IF lower(query_text) ~ '(insert|update|delete|drop|alter|create|truncate|grant|revoke)' THEN
        RAISE EXCEPTION 'Mutation queries are not allowed';
    END IF;

    EXECUTE format('SELECT jsonb_agg(row_to_json(t)) FROM (%s) t', query_text) INTO result;
    RETURN COALESCE(result, '[]'::jsonb);
END;
$$;
