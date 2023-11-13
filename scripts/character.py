import urllib3
from bs4 import BeautifulSoup
import json
import pandas as pd
import re

base_url = "https://onepiece.fandom.com"


def parse_affiliation(div_element):
    a_elements = div_element.find_all("a")
    affiliations = []
    for a_element in a_elements:
        affiliations.append(a_element.get("title"))
        # TODO: add note for each title, and probably URL

    return affiliations


def parse_generic(div_element):
    text = remove_footnote(div_element.text)
    if ";" in text:
        return [x.strip() for x in text.split(";")]
    else:
        new_div = BeautifulSoup(str(div_element).replace("<br/>", ";"))
        text = remove_footnote(new_div.text)
        return [x.strip() for x in text.split(";")]


def parse_list(div_element):
    li_tags = div_element.find_all("li")
    values = [x.text for x in li_tags]
    # Remove footnotes
    values = [remove_footnote(x) for x in values]
    return values


def parse_content(div_element):
    has_ul = bool(div_element.find("ul"))
    if has_ul:
        return parse_list(div_element)
    else:
        return parse_generic(div_element)


def parse_age(ages):
    values = [remove_note(a) for a in ages]
    return values


def parse_height(heights):
    values = [remove_note(h) for h in heights]
    return values


def parse_birthday(full_birthday):
    return full_birthday


def parse_bounty(bounties):
    values = [b.replace(",", "") for b in bounties]
    return values


def parse_alias(aliases):
    # Removing japanesse notes
    return [remove_note(a) for a in aliases]


def remove_footnote(text):
    return re.sub(r"\[[0-9]+\]", "", text)


def remove_note(text):
    return text.split("(")[0].strip()


def scrap_character(character_url: str):
    """Scrap character."""
    character_info = {}

    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen("GET", character_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, "html.parser")

    character_sections = soup.findAll(
        "section",
        {"class": "pi-item pi-group pi-border-color pi-collapse pi-collapse-open"},
    )

    for character_section in character_sections:
        div_elements = character_section.find_all("div", {"data-source": True})
        # print(f"Number of div in section: {len(div_elements)}")
        i = 0
        for div_element in div_elements:
            data_source = div_element.get("data-source")
            # title = div_element.find_all("h3")[0].text
            div_content = div_element.find_all(
                "div", {"class": "pi-data-value pi-font"}
            )[0]

            # if data_source == "age":
            #     value = parse_content(div_content)
            #     value = parse_age(value)
            # elif data_source == "height":
            #     value = parse_content(div_content)
            #     value = parse_height(value)
            if data_source == "bounty":
                value = parse_content(div_content)
                value = parse_bounty(value)
            elif data_source == "alias" or data_source == "epithet":
                value = parse_content(div_content)
                value = parse_alias(value)
            # elif data_source == "occupation":
            #     value = parse_generic(div_content)
            else:
                value = parse_content(div_content)
            # print(i, data_source, value)
            character_info[data_source] = value
            i += 1
    return character_info


def parse_all_characters():
    file_path = "./data/characters.csv"
    df = pd.read_csv(file_path)

    df_head = df.head(len(df))

    characters = {}

    for index, row in df_head.iterrows():
        full_url = base_url + row["url"]
        print(f"{index}. {row['name']} - {row['id']}")
        try:
            characters[row["id"]] = scrap_character(full_url)
        except Exception as e:
            print(f'>>>>>> Failed on {row["name"]} {full_url} because {e} ')

        if index % 100 == 0:
            with open("./cache/characters_{}.json".format(index), "w") as fp:
                json.dump(characters, fp)

    with open("./data/characters_detail.json", "w") as fp:
        json.dump(characters, fp, indent=2)


def pretty_print(value: dict):
    for k, v in value.items():
        print(k, v)


if __name__ == "__main__":
    url = "https://onepiece.fandom.com/wiki/Sanji"

    result = scrap_character(url)
    pretty_print(result)

    # parse_all_characters()

    print("fin")
