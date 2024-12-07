import duckdb
import json
import pandas as pd


def create_db(duckdb_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(duckdb_path)


def create_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("DROP TABLE IF EXISTS coc CASCADE")
    conn.execute("DROP TABLE IF EXISTS chapters CASCADE")
    conn.execute("DROP TABLE IF EXISTS volume CASCADE")
    conn.execute("DROP TABLE IF EXISTS characters CASCADE")

    conn.execute(
        """
        CREATE TABLE volume (
            volume_number INTEGER PRIMARY KEY,
            english_title TEXT,
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE chapters (
            chapter INTEGER PRIMARY KEY,
            volume INTEGER,
            title TEXT,
            pages INTEGER,
            date DATE,
            jump TEXT,
            FOREIGN KEY(volume) REFERENCES volume(volume_number)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE characters (
            id TEXT PRIMARY KEY,
            name TEXT,
            origin TEXT,
            status TEXT,
            birth TEXT,
            blood_type TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE coc (
            chapter INTEGER,
            character TEXT,
            note TEXT,
        FOREIGN KEY(chapter) REFERENCES chapters(chapter),
        FOREIGN KEY(character) REFERENCES characters(id)
        )
        """
    )


def load_volume(conn: duckdb.DuckDBPyConnection, volume_json_path: str):
    with open(volume_json_path, "r") as f:
        volumes = json.load(f)
    for volume in volumes:
        volume_number = volume["volume_number"]
        title = volume["english_title"]
        sql = """
            INSERT INTO volume (volume_number, english_title)
            VALUES (?, ?)
            """
        # print(sql, (volume_number, title))
        conn.execute(sql, (volume_number, title))


def load_chapters(conn: duckdb.DuckDBPyConnection, chapters_csv_path: str):
    chapters = pd.read_csv(chapters_csv_path)
    for _, chapter in chapters.iterrows():
        chapter_number = int(chapter["chapter"])
        volume = chapter["volume"]
        if pd.isna(volume):
            volume = None
        title = chapter["name"]
        pages = int(chapter["page"])
        date = chapter["date"]
        jump = chapter["jump"]
        sql = """
            INSERT INTO chapters (chapter, volume, title, pages, date, jump)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        try:
            conn.execute(sql, (chapter_number, volume, title, pages, date, jump))
        except Exception as e:
            print(e)
            print(sql, (chapter_number, volume, title, pages, date, jump))
            continue


# Return the first item from a list or the item itself if it's not a list
def get_string(attributes, key):
    # TODO: simplify this
    if key not in attributes.keys():
        return None
    else:
        try:
            return (
                attributes[key][0]
                if isinstance(attributes.get(key, None), list)
                else attributes.get(key, None)
            )
        except Exception as e:
            print("get_string error", e, attributes["id"])
            return None


def get_name(attributes):
    name = ""
    if "name" in attributes.keys():
        name = get_string(attributes, "name")
    if not name and "ename" in attributes.keys():
        name = get_string(attributes, "ename")
    if not name:
        name = get_string(attributes, "id")
    return name


def load_characters(conn: duckdb.DuckDBPyConnection, characters_json_path: str):
    with open(characters_json_path, "r") as f:
        characters = json.load(f)
    for character in characters:
        try:
            id = character["id"]
            name = get_name(character)
            origin = get_string(character, "origin")
            status = get_string(character, "status")
            birth = get_string(character, "birth")
            blood_type = get_string(character, "blood type")
            sql = """
                INSERT INTO characters (id, name, origin, status, birth, blood_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """
        except Exception as e:
            print("parsing error", e)
            print(id, name, origin, status, birth, blood_type)
        try:
            conn.execute(sql, (id, name, origin, status, birth, blood_type))
        except Exception as e:
            print("sql error", e, sql)
            continue
