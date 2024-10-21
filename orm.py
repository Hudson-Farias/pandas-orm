import pandas as pd
from pydantic import BaseModel
from typing import Optional


class Base(BaseModel):
    _file_name: Optional[str] = None


    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def __read_csv(self):
        self._file_name = str(self._file_name).replace("default=", "").replace("'", "")

        if not self._file_name: raise 'Not file'

        try: return pd.read_csv(f'data/{self._file_name}')
        except FileNotFoundError: return pd.DataFrame()


    def __save_csv(self, df):
        if not self._file_name: raise 'Not file'

        df = df.drop(columns = ['id'])
        df.index.name = 'id'
        df.to_csv(f'data/{self._file_name}')


    @classmethod
    def insert(cls, **kwargs):
        inst = cls(**kwargs)

        df = inst.__read_csv()
        new_row = pd.DataFrame([inst.dict()])
        
        df = pd.concat([df, new_row], ignore_index = True)
        inst.__save_csv(df)

        return cls.find(cls)


    @classmethod
    def find(cls, **kwargs):
        df = cls.__read_csv(cls)
        if df.empty: return []
        
        items = kwargs.items()

        if items:
            query = ' & '.join([f"{k} == {repr(v)}" for k, v in items])
            df = df.query(query)
        
        data = [cls(**row.to_dict()) for index, row in df.iterrows()]
        return data


    @classmethod
    def update(cls, id, **kwargs):
        df = cls.__read_csv(cls)
        if df.empty: return []

        query = f'id == {repr(id)}'
        filtered_index = df.query(query).index
        
        for idx in filtered_index:
            for key, value in kwargs.items():
                df.at[idx, key] = value

        cls.__save_csv(cls, df)
        return cls.find()


    @classmethod
    def delete(cls, id):
        df = cls.__read_csv(cls)
        
        df = df[df['id'] != id]
        
        cls.__save_csv(cls, df)
        return cls.find()
