import asyncio, time, json
import pandas as pd
from sqlalchemy import create_engine

class Queue:
    def __init__(self, table_names):
        self.tracker, self.program_names, self.cluster_routine, self.tracker = {}, {}, {}, {}
        self.dependency_ids, self.dependency_keys = [], []
        self.table_names = table_names
        self.db_uri = r'postgresql://postgres:admin123@localhost:5432/gic_funds'
        self.db = create_engine(self.db_uri)
        asyncio.run(self.load_data(self.table_names))

    async def process_data(self, table_name):
        match table_name:
            case "prog_name":
                conn = self.db.connect()
                # # some simple data operations
                sql_query = 'SELECT * FROM prog_name'
                table = pd.read_sql(sql_query, self.db)
                data = table.to_dict('list')
                conn.close()
                for i in range(0, len(table)):
                    self.program_names[data['STEP_SEQ_ID'][i]] = data['STEP_PROG_NAME'][i]
                return True
            case "dependency_rule":
                conn = self.db.connect()
                sql_query = 'SELECT * FROM dependency_rule'
                table = pd.read_sql(sql_query, self.db)
                data = table.to_dict('list')
                conn.close()
                for i in range(0, len(table)):
                    dep_id = data['STEP_DEP_ID'][i]
                    self.dependency_ids.append(dep_id)
                    if dep_id not in self.cluster_routine:
                        self.cluster_routine[dep_id] = []
                        self.cluster_routine[dep_id].append({
                            'rule_id': data['RULE_ID'][i],
                            'seq_id': data['STEP_SEQ_ID'][i],
                            'unit_nbr': data['UNIT_NBR'][i]
                        })
                    else:
                        self.cluster_routine[dep_id].append({
                            'rule_id': data['RULE_ID'][i],
                            'seq_id': data['STEP_SEQ_ID'][i],
                            'unit_nbr': data['UNIT_NBR'][i]
                        })
                self.dependency_keys = list(set(self.dependency_ids))
                return True

    async def load_data(self, table_names):
        tasks = []
        start = time.time()
        for name in table_names:
            temp = asyncio.create_task(self.process_data(table_name=name))
            tasks.append(temp)
        check_all = [await x for x in tasks]
        print("Load data check: {}".format(check_all))
        end = time.time()
        print('Execution time for loading data: {}'.format(end-start))
        return True if all(check_all) else False

    async def run_program(self, seq_id, rule_id, dep_id, wait):
        await asyncio.sleep(wait)
        print("RULE ID ({}): {} -> {}".format(rule_id, self.program_names[seq_id], dep_id))
        if seq_id not in self.tracker:
            self.tracker[seq_id] = True
        return True

    async def main(self):
        """
        To run coroutines
        :return:
        """
        start = time.time()
        tasks = []
        counter = 0
        while counter < len(self.dependency_keys):
            if counter == 0:
                for routine in self.cluster_routine[counter]:
                    temp = asyncio.create_task(self.run_program(routine['seq_id'], routine['rule_id'], counter, routine['unit_nbr']))
                    tasks.append(temp)
                counter += 1
            else:
                if counter in self.tracker:
                    for routine in self.cluster_routine[counter]:
                        temp = asyncio.create_task(self.run_program(routine['seq_id'], routine['rule_id'], counter, routine['unit_nbr']))
                        tasks.append(temp)
                    counter += 1
                await asyncio.sleep(0)
        check_all = [await x for x in tasks]
        print(check_all)
        if all(check_all):
            end = time.time()
            print('Total execution time: {}'.format(end-start))

if __name__ == '__main__':
    table_names = ['prog_name', 'dependency_rule']
    queue = Queue(table_names=table_names)
    asyncio.run(queue.main())
