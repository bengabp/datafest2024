import random
import time
from typing import Dict, List

import pandas as pd
from tqdm import tqdm
import httpx

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
        self.terms_per_class = 3
        self.subject_divisions = _objects['subject_divisions']
        self.electives = self.subject_divisions['electives']

        with open(os.path.join(BASE_DIR, 'datasets', "names.json")) as f:
            self.gnames = json.load(f)

        _all_subs = set()
        for k, v in self.subjects.items():
            for _v in v:
                _all_subs.add(_v)
        self.all_subjects = list(_all_subs)

    def fill_subjects_with_electives(self, subjects: List) -> List:
        while len(subjects) <= 9:
            elective = random.choice(self.electives)
            if elective not in subjects:
                subjects.append(elective)
        return subjects

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

    def generate_teachers_time_allocation_data(self):
        """
        Generates synthetic data representing hours spent by each teacher teaching their subject (in percentage)

        :return: None
        """

        data = supabase.table('teachers').select('id', 'subject_id').execute()
        for teacher in data.data:
            for class_num in range(1, len(self.classes)+1):
                # Condition ensures term is 3 as long as class is not sss3 and jss3 cos they have only one term
                for term in range(1, self.terms_per_class + 1 if class_num not in [2, 6] else 2):
                    percentage_taught = round(random.uniform(0.1, 1.1) * 100, 2)
                    time_allocation_data = {
                        "teacher_id": teacher["id"],
                        "class_id": class_num,
                        "term_id": term,
                        "subject_id": teacher["subject_id"],
                        "hours": percentage_taught
                    }
                    supabase.table('time_allocation').insert(time_allocation_data).execute()

    def update_student_subjects(self):
        """
        Updates subjects offered by a student in senior secondary as from ss2
        :return:
        """

        data = supabase.table("subjects").select('id', 'subject_name').execute()
        subject_mapping = {subject['subject_name']: subject['id'] for subject in data.data}
        data = supabase.table('students').select('student_id').execute()
        for student in data.data:
            # Decide if student is science or art:
            course = random.choice(['science', 'art'])
            subjects = [subject_mapping[s] for s in self.fill_subjects_with_electives(self.subject_divisions[course])]
            (
                supabase.table('students')
               .update({'subjects': subjects, "course": course})
               .eq("student_id", student['student_id'])
               .execute())

    def calculate_score(self, income_level, teaching_percentage, assessment_type, max_income_level):
        income_influence = 0.8 + (income_level / max_income_level) * 0.4
        hours_influence = teaching_percentage / 100
        base_scores = {
            "test": random.randint(40, 70),
            "homework": random.randint(50, 80),
            "exam": random.randint(50, 90),
            "class_test": random.randint(45, 75)
        }
        base_score = base_scores[assessment_type]
        score = base_score * income_influence * hours_influence
        return min(100, max(0, score + random.uniform(-5, 5)))

    def generate_students_accessment_data(self):
        # Find max income_level

        max_income_level = 0
        data = supabase.table('students').select('student_id', 'parents(income_level)', 'subjects', 'course').order('income_level', desc=True, foreign_table='parents').execute()

        ASSESSMENT_TYPES = ["test", "homework", "exam"]
        records = []

        for _index, student in tqdm(enumerate(data.data)):
            if _index == 0:
                max_income_level = student['parents']["income_level"]

            for class_num in range(1, len(self.classes) + 1):
                for term in range(1, self.terms_per_class + 1 if class_num not in [2, 6] else 2):
                    for subject in student['subjects']:
                        while 1:
                            try:
                                time_allocation = supabase.table('time_allocation').select('hours').match({
                                    "subject_id": subject,
                                    "term_id": term,
                                    "class_id": class_num
                                }).execute()
                                break
                            except (httpx.RemoteProtocolError, httpx.TimeoutException):
                                print("Failed to fetch time allocation data, retrying...")
                                time.sleep(1)

                        teaching_percentage = time_allocation.data[0]['hours']
                        for assessment_type in ASSESSMENT_TYPES:
                            score = self.calculate_score(student['parents']["income_level"], teaching_percentage, assessment_type, max_income_level)
                            assessment_data = {
                                "student_id": student["student_id"],
                                "class_id": class_num,
                                "term_id": term,
                                "subject_id": subject,
                                "assessment_type": assessment_type,
                                "score": round(score, 2)
                            }
                            # Insert into assessments table
                            records.append(assessment_data)
                            # while 1:
                            #     try:
                            #         supabase.table("assessments").insert(assessment_data).execute()
                            #         break
                            #     except (
                            #             httpx.RemoteProtocolError,
                            #             httpx.TimeoutException,
                            #             httpx.RequestError,
                            #             httpx.NetworkError,
                            #             httpx.HTTPStatusError,
                            #     ):
                            #         print("Failed to insert assessment data, retrying...")
                            #         time.sleep(1)
                            #         continue
        df = pd.DataFrame(records)
        df.to_csv("datasets/assessments.csv", index=None)


if __name__ == "__main__":
    manager = DataManager()
    manager.generate_students_accessment_data()
    print(manager.classes)
