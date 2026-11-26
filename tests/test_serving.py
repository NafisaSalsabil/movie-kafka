import os
import tempfile
import unittest
from database import connect, load_batch, load_hourly
from queries import answer, benchmark

class ServingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name,'test.sqlite3')
        load_batch([(1,'A'),(2,'B'),(3,'C')],[(1,4.8,1000),(2,5.0,999),(3,4.8,1100)],[(2020,'Drama',4),(2020,'Comedy',4),(2021,'Drama',2)],self.path)
    def tearDown(self):
        self.temp.cleanup()
    def test_questions_threshold_ties_latest_event_hour(self):
        self.assertEqual([r['movie_id'] for r in answer(1,self.path)['rows']],[1,3])
        self.assertEqual(len(answer(2,self.path)['rows']),3)
        load_hourly([('2020-01-01 00:00:00','2020-01-01 01:00:00',1,100),('2020-01-01 01:00:00','2020-01-01 02:00:00',3,2)],0,self.path)
        self.assertEqual(answer(3,self.path)['rows'][0]['movie_id'],3)
    def test_replay_and_absolute_updates(self):
        row=('2020-01-01 00:00:00','2020-01-01 01:00:00',1,2)
        self.assertEqual(load_hourly([row],0,self.path),1)
        self.assertEqual(load_hourly([row],0,self.path),0)
        load_hourly([(*row[:3],5)],1,self.path)
        self.assertEqual(answer(3,self.path)['rows'][0]['rating_count'],5)
    def test_failed_sink_rolls_back_ledger_and_rows(self):
        def broken():
            yield ('2020-01-01 00:00:00','2020-01-01 01:00:00',1,2)
            raise RuntimeError('simulated crash')
        with self.assertRaises(RuntimeError):
            load_hourly(broken(),7,self.path)
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM hourly_counts').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sink_batches').fetchone()[0],0)
    def test_benchmark_and_batch_reload(self):
        results=benchmark(self.path)
        self.assertEqual(len(results),6)
        self.assertTrue(all(r['elapsed_ms']>=0 for r in results))
        with connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM query_timings').fetchone()[0],6)
    def test_empty_stream(self):
        self.assertEqual(answer(3,self.path)['status'],'no_results_yet')

    def test_batch_reload_replaces_stale_results_preserves_stream(self):
        load_hourly([('2020-01-01 00:00:00','2020-01-01 01:00:00',1,2)],0,self.path)
        load_batch([(1,'Renamed')],[(1,4.0,1001)],[(2022,'Drama',1)],self.path)
        self.assertEqual(answer(1,self.path)['rows'][0]['title'],'Renamed')
        self.assertEqual(len(answer(2,self.path)['rows']),1)
        self.assertEqual(answer(3,self.path)['rows'][0]['rating_count'],2)
    def test_failed_batch_load_is_atomic(self):
        with self.assertRaises(Exception):
            load_batch([(1,'A'),(1,'Duplicate')],[],[],self.path)
        self.assertEqual(len(answer(1,self.path)['rows']),2)
    def test_benchmark_failure_restores_indexes(self):
        from unittest.mock import patch
        from database import INDEXES
        with patch('queries.answer',side_effect=RuntimeError('simulated query error')):
            with self.assertRaises(RuntimeError):
                benchmark(self.path)
        with connect(self.path) as db:
            names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertTrue(set(INDEXES).issubset(names))

if __name__=='__main__':
    unittest.main()
