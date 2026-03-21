// Purple Carrot (QwxIISziQhWR) | DLVE Universal Tag | v14.11.0
// FIX v14.11.0: Removed cart commit fetch intercept (/api/v2/carts/?commit=true)
//   that was capturing cart save/modify activity as purchase events.
//   Now ONLY the ordersuccess dataLayer event triggers purchase tracking,
//   using transactionId and transactionTotal from the dataLayer payload.
(function(){
const ADVERTISER_ID="1060";
const CONVERSION_ID="QwxIISziQhWR";
const ENDPOINT_URL="https://data.script.flowershop.media";
const STATIC_JS_URL="https://dlyyrzii9sh86.cloudfront.net/js_include/global_dlve.js?v=2.6.6";
const SAQ_PIXEL_KEY="xt5chVHtrnw0wJZcvE7Ael";
const SAQ_RT_SID="5dEaYPe2HyCbyWjEIxHtem";
const SCRIPT_VERSION="v14.11.0";
const SCRIPT_TYPE="Universal DLVE";
/* =========================================================
   GLOBAL INIT
========================================================= */
if(window.__dlve_pc_loaded__) return;
window.__dlve_pc_loaded__=true;
window.advertiser_id=ADVERTISER_ID;
window.dojoPixelKey_Conv=CONVERSION_ID;
window.script_type=SCRIPT_TYPE;
window.scriptVersion=SCRIPT_VERSION;
if(!window.__global_dlve_injected__){
  window.__global_dlve_injected__=true;
  const s=document.createElement("script");
  s.src=STATIC_JS_URL;
  s.async=true;
  document.head.appendChild(s);
}
/* =========================================================
   HELPERS
========================================================= */
const cap=(v,n=2000)=>String(v||"").slice(0,n);
const parseNum=v=>parseFloat(String(v||"").replace(/[^\d.]/g,""))||0;
const round2=n=>Math.round(n*100)/100;
const toISO=()=>new Date().toISOString();
const normalizeItems=a=>
  Array.isArray(a)?a.map(p=>({
    item_id:p?.item_id||p?.id||"",
    item_name:p?.item_name||p?.name||"",
    item_category:p?.item_category||p?.category||"",
    price:round2(parseNum(p?.price)),
    quantity:parseNum(p?.quantity??1)
  })) : [];
const getVisitor=()=>{
  try{
    const id=window.getVisitorId?.()||"";
    const status=id&&window.getVisitorStatus
      ? (window.getVisitorStatus.length
          ?window.getVisitorStatus(id)
          :window.getVisitorStatus())||"unknown"
      :"unknown";
    return {visitor_id:id,visitor_status:status};
  }catch{
    return {visitor_id:"",visitor_status:"unknown"};
  }
};
/* =========================================================
   IDENTITY CAPTURE
========================================================= */
let identity={
  email_hash:"",
  customer_id:"",
  zip_code:""
};
let loggedInStatus="";
async function sha256(str){
  try{
    const buf=new TextEncoder().encode(str.trim().toLowerCase());
    const hash=await crypto.subtle.digest("SHA-256",buf);
    return Array.from(new Uint8Array(hash))
      .map(b=>b.toString(16).padStart(2,"0"))
      .join("");
  }catch{return "";}
}
async function setIdentityFromEmail(email){
  try{
    if(email && !identity.email_hash){
      identity.email_hash=await sha256(String(email));
    }
  }catch{}
}
async function handleUser(user){
  try{
    if(!user?.email) return;
    identity.customer_id=String(user.id||"");
    identity.zip_code=String(user.zipCode||"");
    if(!identity.email_hash){
      identity.email_hash=await sha256(user.email);
    }
  }catch{}
}
/* =========================================================
   PAYLOAD BUILDER
========================================================= */
function buildPayload(evt){
  const visitor=getVisitor();
  const ecommerce=evt?.ecommerce||{};
  const items=normalizeItems(ecommerce.items||[]);
  const revenue=round2(parseNum(ecommerce.value));
  const cartQty=items.reduce((s,p)=>s+p.quantity,0);
  const orderId=
    ecommerce.transaction_id||
    evt?.transaction_id||
    evt?.transactionId||
    evt?.id||
    "";
  const emailHash=evt?.email_hash||identity.email_hash||"";
  const customerId=evt?.customer_id||identity.customer_id||"";
  const zipCode=evt?.zip_code||identity.zip_code||"";
  return {
    ts:toISO(),
    event:"purchase",
    advertiser_id:ADVERTISER_ID,
    script_id:CONVERSION_ID,
    script_type:SCRIPT_TYPE,
    script_version:SCRIPT_VERSION,
    visitor_id:visitor.visitor_id,
    visitor_status:visitor.visitor_status,
    logged_in:loggedInStatus,
    email_hash:emailHash,
    customer_id:customerId,
    zip_code:zipCode,
    event_url:cap(location.href),
    referrer:cap(document.referrer),
    cart_quantity:cartQty,
    dedupe_id:String(orderId),
    revenue:revenue,
    value:revenue,
    currency:ecommerce.currency||"USD",
    products:items
  };
}
/* =========================================================
   RELIABLE DELIVERY (sendBeacon upgrade)
========================================================= */
async function sendDLVE(payload){
  const body=JSON.stringify(payload);
  try{
    if(navigator.sendBeacon){
      const blob=new Blob([body],{type:"application/json"});
      if(navigator.sendBeacon(ENDPOINT_URL,blob)) return true;
    }
  }catch{}
  try{
    const qs=new URLSearchParams({
      data:btoa(unescape(encodeURIComponent(body)))
    }).toString();
    new Image().src=
      ENDPOINT_URL+"?beacon=1&"+qs+"&_ts="+Date.now();
    return true;
  }catch{}
  try{
    await fetch(ENDPOINT_URL,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body,
      keepalive:true
    });
    return true;
  }catch{return false;}
}
function sendSA(payload){
  if(!SAQ_PIXEL_KEY) return;
  try{
    const p=new URLSearchParams();
    p.set("cid",SAQ_PIXEL_KEY);
    Object.keys(payload).forEach(k=>{
      p.set("sa_conv_data_"+k,String(payload[k]));
    });
    new Image().src=
      "https://tags.srv.stackadapt.com/conv?"+
      p.toString()+"&_ts="+Date.now();
  }catch{}
}
function sendSA_RT(payload){
  if(!SAQ_RT_SID) return;
  try{
    const p=new URLSearchParams();
    p.set("sid",SAQ_RT_SID);
    p.set("saq_event","purchase");
    if(payload.revenue) p.set("saq_revenue",String(payload.revenue));
    if(payload.dedupe_id) p.set("saq_order_id",String(payload.dedupe_id));
    new Image().src=
      "https://tags.srv.stackadapt.com/rt?"+
      p.toString()+"&_ts="+Date.now();
  }catch{}
}
/* =========================================================
   DEDUPE
========================================================= */
const PREFIX="dlve_purchase_"+CONVERSION_ID+"_";
function fired(id){
  try{return sessionStorage.getItem(PREFIX+id)==="1";}
  catch{return false;}
}
function mark(id){
  try{sessionStorage.setItem(PREFIX+id,"1");}
  catch{}
}
/* =========================================================
   PURCHASE PROCESSOR
   Only fires on real ordersuccess dataLayer events (v14.11.0)
========================================================= */
async function processPurchase(evt){
  try{
    if(evt?.email && !identity.email_hash){
      await setIdentityFromEmail(evt.email);
    }
  }catch{}
  /* ── identity retry for timing gap ──────── */
  const needsIdentity=
    loggedInStatus.startsWith("YES") &&
    !identity.email_hash &&
    !evt?.email_hash;
  if(needsIdentity){
    await new Promise(r=>setTimeout(r,500));
  }
  const payload=buildPayload(evt);
  const id=payload.dedupe_id;
  if(!id||fired(id)) return;
  mark(id);
  await sendDLVE(payload);
  sendSA_RT(payload);
  if(payload.revenue>0 && payload.products.length){
    sendSA({...payload,products:JSON.stringify(payload.products)});
  }
}
/* =========================================================
   DATA LAYER LISTENER
   Listens for ordersuccess dataLayer event and reads
   transactionId + transactionTotal from the event payload.
========================================================= */
function mapOrderSuccessToEvent(evt){
  const txId = evt?.["DLV-TransactionID"] || evt?.transaction_id || evt?.transactionId || evt?.transactionID || evt?.order_id || evt?.orderId || evt?.id || "";
  const total = evt?.transaction_total ?? evt?.transactionTotal ?? evt?.total ?? evt?.value ?? evt?.revenue ?? "";
  const currency = evt?.currency || "USD";
  const email = evt?.email || evt?.user_email || evt?.userEmail || "";
  const zip = evt?.zip || evt?.zipCode || evt?.postal || evt?.postalCode || "";
  /* ── prefer native ecommerce items from dataLayer ────── */
  const nativeItems = evt?.ecommerce?.items || evt?.items || [];
  /* ── also check transactionProducts (enhanced ecommerce) ── */
  const txProducts = evt?.transactionProducts || [];
  const items = nativeItems.length > 0
    ? nativeItems
    : txProducts.length > 0
      ? txProducts
      : [{
          item_id:"purple_carrot_subscription",
          item_name:"Subscription",
          item_category:"subscription",
          price: total,
          quantity:1
        }];
  return {
    ecommerce:{
      transaction_id: txId,
      value: total,
      currency: currency,
      items: items
    },
    email: email,
    zip_code: String(zip||""),
    customer_id: evt?.user_id || evt?.userId || evt?.customer_id || evt?.customerId || ""
  };
}
function mapAccountCreated(evt){
  const email = evt?.email || evt?.user_email || evt?.userEmail || "";
  const txId = evt?.transaction_id || evt?.transactionId || evt?.transactionID || evt?.id || "";
  if(email) setIdentityFromEmail(email);
  if(txId){
    try{ sessionStorage.setItem("dlve_pc_pending_tx", String(txId)); }catch{}
  }
}
function handleDataLayerEvent(evt){
  try{
    if(evt?.logged_in !== undefined && !loggedInStatus){
      loggedInStatus = String(evt.logged_in);
    }
    const name = String(evt?.event||"").toLowerCase();
    if(!name) return;
    if(name==="account_created"){
      mapAccountCreated(evt);
      return;
    }
    if(name==="ordersuccess"){
      const mapped = mapOrderSuccessToEvent(evt);
      processPurchase(mapped);
      return;
    }
  }catch{}
}
function hookDataLayer(){
  try{
    if(!window.dataLayer || window.__dlve_pc_datalayer_hooked__) return;
    window.__dlve_pc_datalayer_hooked__=true;
    const origPush = window.dataLayer.push;
    window.dataLayer.push = function(){
      const args=[].slice.call(arguments);
      for(let i=0;i<args.length;i++){
        try{ handleDataLayerEvent(args[i]); }catch{}
      }
      return origPush.apply(window.dataLayer,args);
    };
    try{
      for(let j=0;j<window.dataLayer.length;j++){
        handleDataLayerEvent(window.dataLayer[j]);
      }
    }catch{}
  }catch{}
}
/* =========================================================
   FETCH INTERCEPT (IDENTITY ONLY)
   v14.11.0: Removed cart commit intercept. Only /users/me
   is intercepted for identity capture (email, customer_id).
   Cart saves (/api/v2/carts/?commit=true) are NO LONGER
   treated as purchase events.
========================================================= */
const origFetch=window.fetch;
window.fetch=async function(){
  const res=await origFetch.apply(this,arguments);
  try{
    const url=arguments[0];
    if(typeof url==="string"){
      if(url.includes("/api/v2/users/me")){
        res.clone().json().then(handleUser).catch(()=>{});
      }
      /* v14.11.0: cart commit intercept REMOVED
         Cart saves were being captured as purchases.
         Only the ordersuccess dataLayer event should
         trigger purchase tracking. */
    }
  }catch{}
  return res;
};
/* =========================================================
   INIT
========================================================= */
(function init(){
  const t=()=>{
    hookDataLayer();
    if(!window.__dlve_pc_datalayer_hooked__) setTimeout(t,50);
  };
  t();
})();
setTimeout(()=>{
  console.log("[DLVE] Purple Carrot "+SCRIPT_VERSION+" initialized");
},800);
})();
