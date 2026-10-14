/* Shared by login, signup, the role-based dashboard and Trace. */
window.UzoAuth = (() => {
 const base=window.UZOAGRO_API_BASE||(location.hostname.endsWith('vercel.app')?'https://uzoagroai-uzoagro-api.hf.space':'');
 const token=()=>sessionStorage.getItem('uzoagro_token');
 function clear(){sessionStorage.removeItem('uzoagro_token');localStorage.removeItem('uzoagro_user');sessionStorage.removeItem('trace_token');localStorage.removeItem('trace_user');}
 function save(result){if(!result.token||!result.user?.role)throw Error('The backend needs the unified account update before this workspace can sign you in.');sessionStorage.setItem('uzoagro_token',result.token);localStorage.setItem('uzoagro_user',JSON.stringify(result.user));sessionStorage.removeItem('trace_token');localStorage.removeItem('trace_user');return result.user;}
 async function request(path,method='GET',data){const headers={};if(token())headers.Authorization='Bearer '+token();if(data)headers['Content-Type']='application/json';const response=await fetch(base+path,{method,headers,body:data?JSON.stringify(data):undefined});let body;try{body=await response.json();}catch(_){throw Error('The UzoAgro backend could not return a response.');}if(!response.ok){if(response.status===401)clear();throw Error(typeof body.detail==='string'?body.detail:'Check your details and try again.');}return body;}
 const loginUrl=(next='dashboard.html')=>'login.html?next='+encodeURIComponent(next);
 async function me(){if(!token())return null;const result=await request('/api/me');localStorage.setItem('uzoagro_user',JSON.stringify(result.user));return result.user;}
 async function requireUser(next='dashboard.html'){if(!token()){location.replace(loginUrl(next));return null;}try{return await me();}catch(err){if(!token())location.replace(loginUrl(next));else{document.body.innerHTML='';const message=document.createElement('p');message.style.cssText='padding:40px;font-family:Manrope,sans-serif';message.textContent=err.message+' Refresh to retry.';document.body.appendChild(message);}return null;}}
 function destination(user){const requested=new URLSearchParams(location.search).get('next');return user.role==='farmer'&&requested&&/^trace\.html(?:#(?:overview|batches|new|diagnose|account|batch\/[A-Z0-9-]+))?$/.test(requested)?requested:'dashboard.html';}
 async function logout(){try{await request('/api/logout','POST');}finally{clear();location.href='login.html';}}
 return {base,token,clear,save,request,me,requireUser,loginUrl,destination,logout};
})();
