import sqlite3
import unittest
from datetime import date, timedelta
from unittest.mock import patch
import test_trace as fixtures
from test_trace import NeonConnection
from logistics_fixture import SCHEMA


class LogisticsFlow(unittest.TestCase):
    def setUp(self):
        fixtures.TraceJourney.setUp(self)
        conn=sqlite3.connect(self.neon_path); conn.executescript(SCHEMA); conn.commit(); conn.close()
        self.logistics_patch=patch('logistics_api.get_db_connection', lambda: NeonConnection(self.neon_path))
        self.logistics_patch.start()
        self.driver_phone='08087654321'
        self.driver_headers=self.new_account('driver',self.driver_phone)
        self.other_farmer=self.new_account('farmer','08099999999')
        self.cargo={'pickup_city':'Onitsha','dropoff_city':'Lagos','crop':'Cassava','tonnes':.25,'requested_date':date.today().isoformat()}

    def tearDown(self):
        self.logistics_patch.stop(); fixtures.TraceJourney.tearDown(self)

    def new_account(self,role,phone):
        response=self.client.post('/api/signup',json=self.account|{'role':role,'phone':phone,'primary_city':'Onitsha'})
        self.assertEqual(response.status_code,200,response.text)
        return {'Authorization':'Bearer '+response.json()['token']}

    def publish(self,**updates):
        response=self.client.post('/api/logistics/routes',headers=self.driver_headers,json={'origin':'Onitsha','destination':'Lagos','available_date':date.today().isoformat(),'capacity':5,'crops':['Cassava','Maize']}|updates)
        self.assertEqual(response.status_code,200,response.text)
        return response.json()['id']

    def booking(self,route,headers=None):
        response=self.client.post('/api/logistics/jobs',headers=headers or self.headers,json=self.cargo|{'route_id':route})
        self.assertEqual(response.status_code,200,response.text)
        return response.json()['id']

    def test_request_accept_pickup_delivery_and_personal_charts(self):
        route=self.publish()
        response=self.client.post('/api/logistics/matches',headers=self.headers,json=self.cargo)
        self.assertEqual(response.status_code,200,response.text)
        matches=response.json()['matches'];self.assertEqual(matches[0]['id'],route)
        self.assertNotIn('driver_phone',matches[0]);self.assertNotIn(self.driver_phone,str(matches))
        job=self.booking(route)
        self.assertNotIn('contact',self.client.get('/api/logistics/jobs',headers=self.headers).json()[0])
        self.assertEqual(self.client.patch('/api/logistics/jobs/'+job,headers=self.headers,json={'action':'accept'}).status_code,403)
        for action in ('accept','pickup','deliver'):
            r=self.client.patch('/api/logistics/jobs/'+job,headers=self.driver_headers,json={'action':action})
            self.assertEqual(r.status_code,200,r.text)
        jobs=self.client.get('/api/logistics/jobs',headers=self.headers).json()
        self.assertEqual(jobs[0]['status'],'delivered');self.assertEqual(jobs[0]['contact']['phone'],self.driver_phone)
        summary=self.client.get('/api/logistics/dashboard',headers=self.headers).json()
        self.assertEqual(summary['delivered'],1);self.assertEqual(summary['tonnes_delivered'],.25)
        self.assertEqual(summary['months'][-1]['requests'],1);self.assertEqual(summary['months'][-1]['deliveries'],1)
        self.assertEqual(self.client.get('/api/logistics/jobs',headers=self.other_farmer).json(),[])
        self.assertEqual(self.client.get('/api/logistics/dashboard',headers=self.other_farmer).json()['total'],0)

    def test_reservation_duplicate_requests_and_cancellation(self):
        route=self.publish();first=self.booking(route);second=self.booking(route,self.other_farmer)
        self.assertEqual(self.client.post('/api/logistics/jobs',headers=self.headers,json=self.cargo|{'route_id':route}).status_code,409)
        self.assertEqual(self.client.patch('/api/logistics/jobs/'+first,headers=self.other_farmer,json={'action':'cancel'}).status_code,404)
        self.assertEqual(self.client.patch('/api/logistics/jobs/'+first,headers=self.driver_headers,json={'action':'accept'}).status_code,200)
        self.assertEqual(self.client.get('/api/logistics/jobs',headers=self.other_farmer).json()[0]['status'],'declined')
        self.assertEqual(self.client.patch('/api/logistics/jobs/'+second,headers=self.driver_headers,json={'action':'accept'}).status_code,409)
        self.assertEqual(self.client.patch('/api/logistics/jobs/'+first,headers=self.headers,json={'action':'cancel'}).status_code,200)
        self.assertEqual(self.client.get('/api/logistics/routes',headers=self.driver_headers).json()[0]['status'],'available')

    def test_capacity_cargo_timing_direction_and_roles(self):
        self.publish()
        for changed in ({'tonnes':6},{'crop':'Tomatoes'},{'requested_date':(date.today()+timedelta(days=3)).isoformat()}, {'pickup_city':'Lagos','dropoff_city':'Onitsha'}):
            r=self.client.post('/api/logistics/matches',headers=self.headers,json=self.cargo|changed)
            self.assertEqual(r.status_code,200,r.text);self.assertEqual(r.json()['matches'],[])
        self.assertEqual(self.client.post('/api/logistics/routes',headers=self.headers,json={'origin':'Onitsha','destination':'Lagos','available_date':date.today().isoformat(),'capacity':5,'crops':['Cassava']}).status_code,403)
        self.assertEqual(self.client.post('/api/logistics/matches',headers=self.driver_headers,json=self.cargo).status_code,403)
        self.assertEqual(self.client.get('/api/logistics/dashboard').status_code,401)

    def test_linked_batch_ownership_and_quantity(self):
        batch=self.client.post('/api/trace/batches',headers=self.headers,json={'crop':'Cassava','quantity':250,'community':'Onitsha','harvest_date':date.today().isoformat()}).json()
        route=self.publish();cargo=self.cargo|{'route_id':route,'batch_id':batch['id']}
        self.assertEqual(self.client.post('/api/logistics/jobs',headers=self.other_farmer,json=cargo).status_code,403)
        self.assertEqual(self.client.post('/api/logistics/jobs',headers=self.headers,json=cargo|{'tonnes':1}).status_code,422)
        self.assertEqual(self.client.post('/api/logistics/jobs',headers=self.headers,json=cargo).status_code,200)

if __name__=='__main__': unittest.main()
