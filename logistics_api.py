"""Authenticated farmer-to-driver requests, capacity reservations and personal analytics."""
from collections import Counter
from datetime import date, datetime, timezone
import math
import secrets

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Literal
from psycopg2.errors import UniqueViolation
from auth_api import current_user, farmer
from database import get_db_connection
from geography import CITIES
from matching_engine import run_matching_engine, extract_affinity_score

router = APIRouter(prefix='/api/logistics', tags=['Logistics'])
CROPS = ['Cassava', 'Maize', 'Tomatoes', 'Yam', 'Rice', 'Plantain', 'Beans', 'Onions']
STATUSES = ['requested', 'accepted', 'picked_up', 'delivered', 'declined', 'cancelled']

def now(): return datetime.now(timezone.utc).isoformat()
def driver(account=Depends(current_user)):
    if account['role'] != 'driver': raise HTTPException(403, 'Only driver accounts can manage return routes.')
    return account

class Cargo(BaseModel):
    pickup_city: str
    dropoff_city: str
    crop: str
    tonnes: float = Field(gt=0, le=100)
    requested_date: date
    batch_id: str | None = None

class NewRoute(BaseModel):
    origin: str
    destination: str
    available_date: date
    capacity: float = Field(gt=0, le=100)
    crops: list[str] = Field(min_length=1, max_length=8)

class Booking(Cargo):
    route_id: str

class JobAction(BaseModel):
    action: Literal['accept', 'decline', 'pickup', 'deliver', 'cancel']

class RouteAction(BaseModel):
    status: Literal['available', 'closed']

def validate_cargo(data):
    if data.pickup_city not in CITIES or data.dropoff_city not in CITIES or data.pickup_city == data.dropoff_city:
        raise HTTPException(422, 'Choose different pickup and destination cities.')
    if data.crop not in CROPS: raise HTTPException(422, 'Choose a supported crop.')
    if data.requested_date < date.today(): raise HTTPException(422, 'Pickup date cannot be in the past.')

def matches_for(data, routes):
    """Keep the existing five-factor engine, with hard safety filters before ranking."""
    validate_cargo(data)
    def fits_corridor(r):
        a,b,p,d=[CITIES[c] for c in (r['origin'],r['destination'],data.pickup_city,data.dropoff_city)]
        vx,vy=b[0]-a[0],b[1]-a[1]; length=vx*vx+vy*vy
        if length==0: return False
        def projection(point): return ((point[0]-a[0])*vx+(point[1]-a[1])*vy)/length
        def deviation(point): return abs(vy*(point[0]-a[0])-vx*(point[1]-a[1]))/math.sqrt(length)
        # Approximate city-scale bounds: pickup within ~165 km, corridor detour ~110 km.
        return math.dist(a,p)<=1.5 and deviation(p)<=1 and deviation(d)<=1 and -.1<=projection(p)<=1.1 and -.1<=projection(d)<=1.1 and projection(d)>projection(p)
    viable = [r for r in routes if r['status'] == 'available' and float(r['capacity']) >= data.tonnes
              and date.fromisoformat(str(r['available_date'])) >= date.today() and fits_corridor(r)
              and abs((date.fromisoformat(str(r['available_date'])) - data.requested_date).days) <= 2
              and extract_affinity_score(pd.Series([r['crops']]), data.crop)[0] > 0]
    # City coordinates are used internally, never sent as precise vehicle positions.
    if not viable: return []
    rows = []
    for r in viable:
        a, b = CITIES[r['origin']], CITIES[r['destination']]
        rows.append({'driver_id':r['id'], 'name':r['driver_name'], 'current_lat':a[0], 'current_lon':a[1],
                     'home_base_lat':b[0], 'home_base_lon':b[1], 'available_capacity':float(r['capacity']),
                     'available_date':pd.Timestamp(r['available_date']), 'allowed_crops':r['crops']})
    p, d = CITIES[data.pickup_city], CITIES[data.dropoff_city]
    req = pd.DataFrame([{'request_id':'preview','pickup_lat':p[0],'pickup_lon':p[1], 'dropoff_lat':d[0],
                         'dropoff_lon':d[1],'required_capacity':data.tonnes,'requested_date':pd.Timestamp(data.requested_date), 'crop_type':data.crop}])
    scores = run_matching_engine(pd.DataFrame(rows), req, top_k=5).to_dict(orient='records')
    by_id = {r['id']:r for r in viable}
    result=[]
    for score in scores:
        r=by_id[score['driver_id']]
        result.append({k:r[k] for k in ('id','driver_name','origin','destination','available_date','capacity','crops')} |
                      {'fit':round(float(score['final_score'])*100), 'factors':{k:round(float(score[k])*100) for k in ('capacity_score','time_score','affinity_score','deadhead_score','corridor_score')}})
    return result

def routes_query(cur):
    cur.execute("SELECT r.*, u.name AS driver_name FROM logistics_routes r JOIN users u ON u.phone=r.driver_phone WHERE u.role='driver' AND r.status='available' AND r.available_date>=%s", (date.today().isoformat(),))
    return cur.fetchall()

def safe_job(row, account):
    item={k:v for k,v in row.items() if k not in ('farmer_phone','driver_phone')}
    if row['status'] in ('accepted','picked_up','delivered'):
        item['contact']={'name':row['driver_name'] if account['role']=='farmer' else row['farmer_name'],
                         'phone':row['driver_phone'] if account['role']=='farmer' else row['farmer_phone']}
    return item

def job_rows(cur, account):
    column='farmer_phone' if account['role']=='farmer' else 'driver_phone'
    cur.execute(f'''SELECT j.*, f.name AS farmer_name, d.name AS driver_name
        FROM logistics_jobs j JOIN users f ON f.phone=j.farmer_phone JOIN users d ON d.phone=j.driver_phone
        WHERE j.{column}=%s ORDER BY j.created_at DESC''', (account['phone'],))
    return cur.fetchall()

@router.get('/config')
def config(account=Depends(current_user)):
    return {'cities':list(CITIES), 'crops':CROPS}

@router.post('/matches')
def find_matches(data: Cargo, account=Depends(farmer)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur: rows=routes_query(cur)
        return {'matches':matches_for(data,rows)}
    finally: conn.close()

@router.post('/routes')
def publish_route(data: NewRoute, account=Depends(driver)):
    if data.origin not in CITIES or data.destination not in CITIES or data.origin==data.destination:
        raise HTTPException(422,'Choose different start and return cities.')
    if data.available_date < date.today(): raise HTTPException(422,'Departure cannot be in the past.')
    if any(c not in CROPS for c in data.crops): raise HTTPException(422,'Choose supported cargo types.')
    route={'id':'RTE-'+secrets.token_hex(5).upper(), 'driver_phone':account['phone'], **data.model_dump(mode='json'),
           'crops':'|'.join(dict.fromkeys(data.crops)), 'status':'available','created_at':now()}
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            columns=list(route)
            cur.execute('INSERT INTO logistics_routes ('+','.join(columns)+') VALUES ('+','.join(['%s']*len(columns))+')', tuple(route.values()))
        conn.commit()
        return {k:v for k,v in route.items() if k!='driver_phone'}
    finally: conn.close()

@router.get('/routes')
def my_routes(account=Depends(driver)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id,origin,destination,available_date,capacity,crops,status,created_at FROM logistics_routes WHERE driver_phone=%s ORDER BY created_at DESC',(account['phone'],))
            return cur.fetchall()
    finally: conn.close()

@router.patch('/routes/{route_id}')
def update_route(route_id: str, data: RouteAction, account=Depends(driver)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM logistics_routes WHERE id=%s AND driver_phone=%s FOR UPDATE',(route_id,account['phone']))
            route=cur.fetchone()
            if not route: raise HTTPException(404,'Route not found.')
            if route['status'] not in ('available','closed'): raise HTTPException(409,'A reserved or completed route cannot be changed.')
            if data.status=='available' and date.fromisoformat(str(route['available_date'])) < date.today(): raise HTTPException(409,'Publish a new route with a current departure date.')
            cur.execute('UPDATE logistics_routes SET status=%s WHERE id=%s',(data.status,route_id))
        conn.commit()
        return {'status':data.status}
    finally: conn.close()

@router.post('/jobs')
def request_pickup(data: Booking, account=Depends(farmer)):
    validate_cargo(data)
    if data.batch_id:
        from trace_api import batch_or_404
        batch=batch_or_404(data.batch_id)
        if batch['owner']!=account['phone']: raise HTTPException(403,'Attach only your own produce batches.')
        if batch['metadata']['crop']!=data.crop: raise HTTPException(422,'The crop must match the attached batch.')
        if data.tonnes*1000 > batch['metadata']['quantity']+0.001: raise HTTPException(422,'Cargo exceeds the attached batch quantity.')
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT r.*,u.name AS driver_name FROM logistics_routes r JOIN users u ON u.phone=r.driver_phone WHERE r.id=%s AND u.role='driver' FOR UPDATE",(data.route_id,))
            route=cur.fetchone()
            if not route or not matches_for(data,[route]): raise HTTPException(409,'This route is no longer a suitable available match. Search again.')
            if route['driver_phone']==account['phone']: raise HTTPException(409,'You cannot request your own return trip.')
            cur.execute("SELECT id FROM logistics_jobs WHERE farmer_phone=%s AND route_id=%s AND status IN ('requested','accepted','picked_up')",(account['phone'],data.route_id))
            if cur.fetchone(): raise HTTPException(409,'You already have an active request for this route.')
            job={'id':'JOB-'+secrets.token_hex(5).upper(), 'farmer_phone':account['phone'], 'driver_phone':route['driver_phone'],
                 **data.model_dump(mode='json'), 'status':'requested','created_at':now(),'updated_at':now()}
            cur.execute('INSERT INTO logistics_jobs ('+','.join(job)+') VALUES ('+','.join(['%s']*len(job))+')',tuple(job.values()))
        conn.commit()
        return {'id':job['id'],'status':'requested'}
    except UniqueViolation:
        conn.rollback(); raise HTTPException(409,'This pickup request already exists.')
    finally: conn.close()

@router.get('/jobs')
def jobs(account=Depends(current_user)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur: rows=job_rows(cur,account)
        return [safe_job(r,account) for r in rows]
    finally: conn.close()

@router.patch('/jobs/{job_id}')
def move_job(job_id: str, data: JobAction, account=Depends(current_user)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM logistics_jobs WHERE id=%s',(job_id,)); initial=cur.fetchone()
            if not initial or account['phone'] not in (initial['farmer_phone'],initial['driver_phone']): raise HTTPException(404,'Shipment not found.')
            # Consistent lock order: route first, then request.
            cur.execute('SELECT * FROM logistics_routes WHERE id=%s FOR UPDATE',(initial['route_id'],)); route=cur.fetchone()
            cur.execute('SELECT * FROM logistics_jobs WHERE id=%s FOR UPDATE',(job_id,)); job=cur.fetchone()
            rules={'accept':('driver','requested','accepted'), 'decline':('driver','requested','declined'),
                   'pickup':('driver','accepted','picked_up'), 'deliver':('driver','picked_up','delivered')}
            if data.action=='cancel':
                if account['role']!='farmer' or account['phone']!=job['farmer_phone'] or job['status'] not in ('requested','accepted'): raise HTTPException(409,'Only the farmer can cancel their request before pickup.')
                target='cancelled'
                if job['status']=='accepted': cur.execute("UPDATE logistics_routes SET status='available' WHERE id=%s",(job['route_id'],))
            else:
                role, expected, target=rules[data.action]
                if account['role']!=role or account['phone']!=job['driver_phone']: raise HTTPException(403,'Only the assigned driver can update this journey.')
                if job['status']!=expected: raise HTTPException(409,'This shipment has changed. Refresh and try again.')
                if data.action=='accept':
                    if route['status']!='available': raise HTTPException(409,'This return trip is already reserved or closed.')
                    cur.execute("UPDATE logistics_routes SET status='booked' WHERE id=%s",(job['route_id'],))
                    cur.execute("UPDATE logistics_jobs SET status='declined',updated_at=%s WHERE route_id=%s AND id<>%s AND status='requested'",(now(),job['route_id'],job_id))
                if data.action=='deliver': cur.execute("UPDATE logistics_routes SET status='completed' WHERE id=%s",(job['route_id'],))
            cur.execute('UPDATE logistics_jobs SET status=%s,updated_at=%s WHERE id=%s',(target,now(),job_id))
        conn.commit(); return {'id':job_id,'status':target}
    finally: conn.close()

@router.get('/dashboard')
def dashboard(account=Depends(current_user)):
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            rows=job_rows(cur,account)
            cur.execute("SELECT COUNT(*) AS count FROM logistics_routes WHERE driver_phone=%s AND status='available' AND available_date>=%s",(account['phone'],date.today().isoformat()))
            published=cur.fetchone()['count']
        counts=Counter(j['status'] for j in rows)
        months=[]
        for back in range(5,-1,-1):
            absolute=date.today().year*12+date.today().month-1-back
            year,month=divmod(absolute,12); key=f'{year:04d}-{month+1:02d}'
            months.append({'month':key,'label':date(year,month+1,1).strftime('%b'),
                           'requests':sum(str(j['created_at']).startswith(key) for j in rows),
                           'deliveries':sum(j['status']=='delivered' and str(j['updated_at']).startswith(key) for j in rows)})
        return {'total':len(rows),'active':counts['accepted']+counts['picked_up'],'pending':counts['requested'],
                'delivered':counts['delivered'],'tonnes_delivered':round(sum(float(j['tonnes']) for j in rows if j['status']=='delivered'),3),
                'published_routes':published,'statuses':{s:counts[s] for s in STATUSES},'months':months,
                'recent':[safe_job(j,account) for j in rows[:5]]}
    finally: conn.close()
