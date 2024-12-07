import configs
from parser.db_creation import (
    create_db,
    create_tables,
    load_volume,
    load_chapters,
    load_coc,
    load_characters,
)


if __name__ == "__main__":
    print("###### Start loading data ######")

    conn = create_db(configs.db_path)
    create_tables(conn)

    load_volume(conn, configs.volumes_json_path)
    load_chapters(conn, configs.chapters_csv)
    load_characters(conn, configs.characters_json_path)
    load_coc(conn, configs.coc_csv)

    conn.close()

    print("Finish loading data")
