import random
from typing import Dict, List
from src.settings import supabase, BASE_DIR
import os
from src.scripts.generate_data import DatasetManager
import json


class DataManager:
    def __init__(self):
        with open(os.path.join(BASE_DIR, "datasets", "objects.json")) as f:
            _objects = json.load(f)
        self.classes = _objects['classes']
        self.occupations = _objects['nigerian_occupations']
        self.subjects = _objects['subjects']
        self.general_subjects = self.subjects['general']
        self.jss_subjects = self.subjects['jss']
        self.sss_subjects = self.subjects['sss']
        self.nigerian_first_names = _objects['nigerian_first_names']

        with open(os.path.join(BASE_DIR, 'datasets', "names.json")) as f:
            self.gnames = json.load(f)

        _all_subs = set()
        for k, v in self.subjects.items():
            for _v in v:
                _all_subs.add(_v)
        self.all_subjects = list(_all_subs)

    def get_random_occupation(self) -> Dict:
        """
        Get random occupation
        :return: Occupation
        """
        return random.choice(self.occupations)

    def insert_subjects_and_teachers(self):
        dsm = DatasetManager()
        all_last_names = list(set(dsm.names_data["igbo"] + dsm.names_data["hausa"] + dsm.names_data["yoruba"]))
        random.shuffle(all_last_names)
        for ind, (fn, ln, subject) in enumerate(zip(self.nigerian_first_names, all_last_names, self.all_subjects)):
            supabase.table('subjects').insert({'id': ind + 1, 'subject_name': subject}).execute()
            supabase.table('teachers').insert({'first_name': fn, 'last_name': ln, 'subject_id': ind + 1, "gender": "male", "years_of_experience": random.randrange(5, 50)}).execute()

    def update_student_firstnames(self):
        """
        Updates the first names of students in the students table
        :return:
        """
        m = 0
        f = 0

        data = supabase.table('students').select('student_id', 'gender').execute()
        for student in data.data:
            if student['gender'] == 'male':
                (
                    supabase.table('students')
                    .update({'first_name': self.gnames['male'][m]})
                    .eq("student_id", student['student_id'])
                    .execute()
                )
                m += 1
                if m > len(self.gnames['male']):
                    m = 0
            else:
                (
                    supabase.table('students')
                    .update({'first_name': self.gnames['female'][f]})
                    .eq("student_id", student['student_id'])
                    .execute())
                f += 1
                if f > len(self.gnames['female']):
                    f = 0

    def update_parent_metadata(self):
        """
        Updates the metadata of parents in the parents table
        :return:
        """

        data = supabase.table('parents').select('parent_id').execute()
        f, m = 0, 0

        for parent in data.data:
            occupation = self.get_random_occupation()
            if parent['parent_id'] % 2:
                (
                    supabase.table('parents')
                    .update({'first_name': self.gnames['male'][m], 'relationship': random.choice(['father', 'nephew', 'uncle']), "occupation": occupation['occupation'], 'income_level': occupation['income_level']})
                    .eq("parent_id", parent['parent_id'])
                    .execute()
                )
                m += 1
                if m > len(self.gnames['male']):
                    m = 0
            else:
                (
                    supabase.table('parents')
                    .update({'first_name': self.gnames['female'][f], 'relationship': random.choice(['mother', 'niece', 'aunt']), "occupation": occupation['occupation'], 'income_level': occupation['income_level']})
                    .eq("parent_id", parent['parent_id'])
                    .execute())
                f += 1
                if f > len(self.gnames['female']):
                    f = 0

    def create_class_tables(self):
        """
        Creates all the tables from jss1-ss3 for storing assessments
        :return:
        """

        pass

    def upsert_subjects(self):
        pass


if __name__ == "__main__":
    manager = DataManager()
    manager.update_parent_metadata()
    print(manager.classes)
