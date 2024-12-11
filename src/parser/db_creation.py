import duckdb
import json
import pandas as pd

from utils import timing_decorator


def create_db(duckdb_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(duckdb_path)


def create_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("DROP TABLE IF EXISTS coc CASCADE")
    conn.execute("DROP TABLE IF EXISTS chapter CASCADE")
    conn.execute("DROP TABLE IF EXISTS volume CASCADE")
    conn.execute("DROP TABLE IF EXISTS character CASCADE")

    conn.execute(
        """
        CREATE TABLE volume (
            number INTEGER PRIMARY KEY,
            title TEXT,
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE chapter (
            number INTEGER PRIMARY KEY,
            volume INTEGER,
            title TEXT,
            num_page INTEGER,
            date DATE,
            jump TEXT,
            FOREIGN KEY(volume) REFERENCES volume(number)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE character (
            id TEXT PRIMARY KEY,
            name TEXT,
            origin TEXT,
            status TEXT,
            birth TEXT,
            blood_type TEXT,
            blood_type_group TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE coc (
            chapter INTEGER,
            character TEXT,
            note TEXT NULL,
        FOREIGN KEY(chapter) REFERENCES chapter(number),
        FOREIGN KEY(character) REFERENCES character(id)
        )
        """
    )


@timing_decorator
def load_volume(conn: duckdb.DuckDBPyConnection, volume_json_path: str):
    with open(volume_json_path, "r") as f:
        volumes = json.load(f)
    num_rows = len(volumes)
    print("[Volume Table] Loading", num_rows, "rows...")
    for volume in volumes:
        number = volume["volume_number"]
        title = volume["english_title"]
        sql = """
            INSERT INTO volume (number, title)
            VALUES (?, ?)
            """
        conn.execute(sql, (number, title))


def parse_volume(volume):
    if not volume or pd.isna(volume) or volume == "":
        return None
    else:
        return int(volume)


@timing_decorator
def load_chapters(conn: duckdb.DuckDBPyConnection, chapters_csv_path: str):
    chapters = pd.read_csv(chapters_csv_path)
    num_rows = chapters.shape[0]
    print("[Chapter Table] Loading", num_rows, "rows...")
    for _, chapter in chapters.iterrows():
        number = int(chapter["chapter"])
        volume = parse_volume(chapter["volume"])
        if pd.isna(volume):
            volume = None
        title = chapter["name"]
        num_page = int(chapter["page"])
        date = chapter["date"]
        jump = chapter["jump"]
        sql = """
            INSERT INTO chapter (number, volume, title, num_page, date, jump)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        try:
            conn.execute(sql, (number, volume, title, num_page, date, jump))
        except Exception as e:
            print(e)
            print(sql, (number, volume, title, num_page, date, jump))
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


def get_blood_type_group(blood_type):
    return blood_type.split(" ")[0]


def parse_blood_type(attributes):
    blood_type = None
    blood_type_group = None
    if "blood type" not in attributes.keys():
        return blood_type, blood_type_group
    if len(attributes["blood type"]) > 1:
        blood_type = ", ".join(attributes["blood type"])
        blood_type_group = "mixed"
    else:
        blood_type = attributes["blood type"][0]
        blood_type_group = get_blood_type_group(blood_type)
    return blood_type, blood_type_group


@timing_decorator
def load_characters(conn: duckdb.DuckDBPyConnection, characters_json_path: str):
    with open(characters_json_path, "r") as f:
        characters = json.load(f)
    num_rows = len(characters)
    print("[Character Table] Loading", num_rows, "rows...")
    for character in characters:
        try:
            id = character["id"]
            name = get_name(character)
            origin = get_string(character, "origin")
            status = get_string(character, "status")
            birth = get_string(character, "birth")
            blood_type, blood_type_group = parse_blood_type(character)
            sql = """
                INSERT INTO character (id, name, origin, status, birth, blood_type, blood_type_group)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
        except Exception as e:
            print("parsing error", e)
            print(character)
        try:
            conn.execute(
                sql, (id, name, origin, status, birth, blood_type, blood_type_group)
            )
        except Exception as e:
            print("sql error", e, sql)
            continue


@timing_decorator
def load_coc(conn: duckdb.DuckDBPyConnection, coc_csv_path: str):
    coc = pd.read_csv(coc_csv_path)
    num_rows = coc.shape[0]
    print("[CoC Table] Loading", num_rows, "rows...")

    # Replace NaN values with an empty string for the 'note' column
    coc["note"] = coc["note"].fillna("")

    # Prepare the SQL statement
    sql = """
        INSERT INTO coc (chapter, character, note)
        VALUES (?, ?, ?)
        """

    # Convert the DataFrame to a list of tuples
    data = coc.values.tolist()

    # Execute the batch insert
    try:
        conn.executemany(sql, data)
        conn.commit()
    except Exception as e:
        print(e)
