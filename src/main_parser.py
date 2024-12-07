from parser.db_creation import (
    create_db,
    create_tables,
    load_volume,
    load_chapters,
    load_characters,
)


if __name__ == "__main__":
    db_path = "./data/op.duckdb"

    conn = create_db(db_path)

    create_tables(conn)

    volumes_json_path = "./data/volumes.json"

    load_volume(conn, volumes_json_path)

    load_chapters(conn, "./data/chapters.csv")

    load_characters(conn, "./data/characters.json")

    conn.close()

    print(f"Database created at {db_path}")
