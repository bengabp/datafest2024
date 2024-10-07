import requests
from selectolax.parser import HTMLParser
import itertools
from src.settings import BASE_DIR
import os
from datetime import datetime
import pandas as pd
import random
import json


class DatasetManager:

    def __init__(self):
        self.names_file = os.path.join(BASE_DIR, "datasets", "ethnic_names.json")

        with open(self.names_file) as f:
            self.names_data = json.load(f
                                        )
        with open(os.path.join(BASE_DIR, "datasets", "male.txt")) as f:
            self.male_names = [line.strip() for line in f]

        with open(os.path.join(BASE_DIR, "datasets", "female.txt")) as f:
            self.female_names = [line.strip() for line in f]

        self.parent_first_names = self.female_names + self.male_names

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
        gender_labels = ['male'] * len(self.male_names) + ['female'] * len(self.female_names)

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
                    "first_name": first,
                    "last_name": last,
                    "gender": gender,
                    "date_of_birth": self.generate_random_dob()
                })
                used_last_names.add(last)

        # Handle case where we still need more names after using each last name once
        remaining_count = num_names - len(unique_names)
        if remaining_count > 0:
            remaining_names = all_name_combinations[:remaining_count]  # Use repeated last names if necessary
            for (first, gender), last in remaining_names:
                unique_names.append({
                    "first_name": first,
                    "last_name": last,
                    "gender": gender,
                    "date_of_birth": self.generate_random_dob()
                })
        df = pd.DataFrame(unique_names)
        df.to_csv(os.path.join(BASE_DIR, "datasets", "names.csv"), index=True, index_label="student_id")
        return df

    def generate_random_dob(self, min_age=9, max_age=11):
        """
        Generates a random date of birth for a student, where the age is between `min_age` and `max_age`.
        """
        # Get the current year
        current_year = datetime.now().year

        # Calculate the range of birth years for the age range
        min_birth_year = current_year - max_age
        max_birth_year = current_year - min_age

        # Generate a random birth year within the range
        random_year = random.randint(min_birth_year, max_birth_year)

        # Generate a random month and day, ensuring the date is valid
        random_month = random.randint(1, 12)
        random_day = random.randint(1, 28)  # To avoid issues with February and leap years

        # Create the random date of birth
        dob = datetime(random_year, random_month, random_day)

        return dob.strftime('%Y-%m-%d')

    def load_students(self, file: str = os.path.join(BASE_DIR, "datasets", "names.csv")) -> pd.DataFrame:
        return pd.read_csv(file)

    def generate_parents_for_students(self, csv_file_path: str = os.path.join(BASE_DIR, "datasets", "students_rows.csv")):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Track used first names to minimize repetition
        used_first_names = set()

        # List to store generated parent names
        parents = []
        parent_ids = []

        for ind, (_, row) in enumerate(df.iterrows()):
            # Filter available names that haven’t been used yet
            available_names = [name for name in self.parent_first_names if name not in used_first_names]

            # If we've used all names, reset to allow repeats
            if not available_names:
                used_first_names.clear()
                available_names = self.parent_first_names

            # Pick a random name from available names and add to used names
            parent_first_name = random.choice(available_names)
            used_first_names.add(parent_first_name)

            # Use the student’s last name for the parent
            parent_last_name = row['last_name']

            # Append parent data to the list
            parents.append({
                "first_name": parent_first_name,
                "last_name": parent_last_name,
                "parent_id": ind
            })
            parent_ids.append(ind)

        # Add generated parents to the DataFrame
        df['parent_id'] = parent_ids
        df.to_csv("datasets/students_p_rows.csv", index=None)
        pd.DataFrame(parents).to_csv("datasets/parents.csv", index=None)

    def generate_teachers(self, num_names=10, save=True) -> pd.DataFrame:

        # Combine all last names from the JSON data
        all_last_names = self.names_data["igbo"] + self.names_data["hausa"] + self.names_data["yoruba"]

        # Expand male and female first names into one list
        all_first_names = self.male_names + self.female_names
        gender_labels = ['male'] * len(self.male_names) + ['female'] * len(self.female_names)

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
                    "first_name": first,
                    "last_name": last,
                    "gender": gender,
                })
                used_last_names.add(last)

        # Handle case where we still need more names after using each last name once
        remaining_count = num_names - len(unique_names)
        if remaining_count > 0:
            remaining_names = all_name_combinations[:remaining_count]  # Use repeated last names if necessary
            for (first, gender), last in remaining_names:
                unique_names.append({
                    "first_name": first,
                    "last_name": last,
                    "gender": gender,
                })
        df = pd.DataFrame(unique_names)
        if save:
            df.to_csv(os.path.join(BASE_DIR, "datasets", "teachers.csv"), index=None)
        return df


if __name__ == "__main__":
    sg = DatasetManager()
