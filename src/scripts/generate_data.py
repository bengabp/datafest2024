import requests
from selectolax.parser import HTMLParser
import itertools
from src.settings import BASE_DIR
import os
import pandas as pd
import random
import json


class StudentGenerator:

    def __init__(self):
        self.names_file = os.path.join(BASE_DIR, "datasets", "ethnic_names.json")

        with open(self.names_file) as f:
            self.names_data = json.load(f
                                        )
        with open(os.path.join(BASE_DIR, "datasets", "male.txt")) as f:
            self.male_names = [line.strip() for line in f]

        with open(os.path.join(BASE_DIR, "datasets", "female.txt")) as f:
            self.female_names = [line.strip() for line in f]

    def scrape_ethnic_names(self):
        names = {}
        links = {
            "igbo": "https://www.behindthename.com/names/gender/unisex/usage/igbo",
            "yoruba": "https://www.behindthename.com/names/gender/unisex/usage/yoruba",
            "hausa": "https://www.behindthename.com/names/gender/unisex/usage/hausa",
        }
        for eth, lnk in links.items():
            res = requests.get(lnk)
            parser = HTMLParser(res.content.decode("utf-8"))
            # Parse the HTML response and extract the ethnic names.
            elements = parser.css("div > div.browsename a.nll[href^='/name/']")
            names[eth] = [e.text().strip() for e in elements if e.text().isalpha()]

        with open(self.names_file, "w") as f:
            json.dump(names, f, indent=4)

    def generate_unique_names_by_gender(self, num_names=300) -> pd.DataFrame:
        # Combine all last names from the JSON data
        all_last_names = self.names_data["igbo"] + self.names_data["hausa"] + self.names_data["yoruba"]

        # Expand male and female first names into one list
        all_first_names = self.male_names + self.female_names
        gender_labels = ['Male'] * len(self.male_names) + ['Female'] * len(self.female_names)

        # Shuffle the first names and gender labels to randomize assignment
        combined_first_names = list(zip(all_first_names, gender_labels))
        random.shuffle(combined_first_names)

        # Generate all combinations of first names and last names
        all_name_combinations = list(itertools.product(combined_first_names, all_last_names))
        random.shuffle(all_name_combinations)  # Shuffle the combinations

        # Check if we have enough unique combinations to generate the requested number of names
        if num_names > len(all_name_combinations):
            raise ValueError("Not enough unique combinations of first names and last names. Consider adding more names.")

        unique_names = []
        used_last_names = set()

        # Create unique name combinations ensuring no last names are repeated unless necessary
        for (first, gender), last in all_name_combinations:
            if len(unique_names) >= num_names:
                break
            if last not in used_last_names:  # Ensure no repeated last names initially
                unique_names.append({
                    "firstname": first,
                    "lastname": last,
                    "gender": gender
                })
                used_last_names.add(last)

        # Handle case where we still need more names after using each last name once
        remaining_count = num_names - len(unique_names)
        if remaining_count > 0:
            remaining_names = all_name_combinations[:remaining_count]  # Use repeated last names if necessary
            for (first, gender), last in remaining_names:
                unique_names.append({
                    "firstname": first,
                    "lastname": last,
                    "gender": gender
                })
        df = pd.DataFrame(unique_names)
        df.to_csv(os.path.join(BASE_DIR, "datasets", "names.csv"), index=False)
        return df

    def load_students(self, file: str = os.path.join(BASE_DIR, "datasets", "names.csv")) -> pd.DataFrame:
        return pd.read_csv(file)


if __name__ == "__main__":
    sg = StudentGenerator()
    data = sg.generate_unique_names_by_gender()
    print(data)
