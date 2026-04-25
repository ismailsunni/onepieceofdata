"""Relation vocabulary for the story graph.

Kept intentionally small. Expand by adding entries and bumping PROMPT_VERSION,
which triggers selective re-extraction in the extraction pipeline.
"""

PROMPT_VERSION = 2

NODE_TYPES = ("character", "crew", "organization", "devil_fruit", "location")

RELATIONS: dict[str, str] = {
    "member_of_crew": "Subject is a member of the crew/group object.",
    "captain_of": "Subject is the captain/leader of the crew/group object.",
    "ally_of": "Subject is an ally or close friend of object (non-familial, non-crewmate).",
    "enemy_of": "Subject is an enemy/antagonist of object.",
    "fought": "Subject fought against object in a notable battle.",
    "defeated_by": "Subject was defeated by object in combat.",
    "mentor_of": "Subject mentored, trained, or taught object.",
    "family_of": "Subject is a blood or adoptive family member of object.",
    "ate_devil_fruit": "Subject consumed the devil fruit object.",
    "affiliated_with": "Subject is affiliated with the organization object.",
    "has_bounty_of": "Subject has a bounty stated in the object (literal value or tier).",
    "originates_from": "Subject originates from the location object (birthplace/hometown).",
}

RELATION_NAMES: tuple[str, ...] = tuple(RELATIONS.keys())
