from pydantic import BaseModel, TypeAdapter
import json
import psycopg2
import time
from psycopg2.extras import execute_values
from bd.config import *
from zipfile import ZipFile


class FirmsFromJson(BaseModel):
    name: str = ''
    full_name: str = ''
    okved: str = ''
    inn: str = ''
    kpp: str = ''
    adress: str = ''

    def __hash__(self):
        return hash(self.inn)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


def form_json_from_zip():
    with ZipFile('egrul.json.zip', 'r') as zip:
        for file_name in zip.namelist()[9000:]:
            with zip.open(file_name) as file:
                json_file = file.read().decode("utf-8")
                json_data = json.loads(json_file)
                for i in json_data:
                    try:
                        if "ХАБАРОВСК" in i['data']['СвАдресЮЛ']['АдресРФ']['Город']['НаимГород']:
                            if i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'].startswith('62'):
                                yield FirmsFromJson.model_validate(
                                    {
                                        'inn': i['data']['ИНН'],
                                        'kpp': i['data']['КПП'],
                                        'name': i['name'],
                                        'full_name': i['full_name'],
                                        'okved': i['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                                        'adress': i['data']['СвРегОрг']['АдрРО']

                                    }
                                )
                    except:
                        continue


if __name__ == "__main__":
    start = time.time()
    data_to_db = TypeAdapter(tuple[FirmsFromJson]).dump_python(tuple(set(form_json_from_zip())))
    print(len(data_to_db))
    with psycopg2.connect(
        user=USER, password=PASSWORD, host="127.0.0.1", port=PORT, database = DBNAME) as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into companys (inn, kpp, name, full_name, okved, adress) values %s on conflict do nothing
            """,
            data_to_db,
            template="""(
            %(inn)s,
            %(kpp)s,
            %(name)s,
            %(full_name)s,
            %(okved)s,
            %(adress)s
            )""",
            page_size=len(data_to_db)
        )
        conn.commit()
    end = time.time()
    print("The time of execution of above program is :",
              (end - start) * 10 ** 3, "ms")
