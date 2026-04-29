// Purple Carrot (QwxIISziQhWR) | DLVE Universal Tag | v15.4.0
// CHANGES v15.4.0: Gift-redemption funnel event.
//   When ordersuccess fires from a URL containing "/redeem_signup/" — i.e.
//   a gift recipient redeeming a code — emits a separate "redeem" funnel event
//   to allEvents. Purchase still gated by isRealOrderId(), so only real
//   297/298/299 IDs land in allPurchases. Redemptions tracked as their own
//   stage so we keep them out of paid-revenue ROAS while still counting
//   attribution against ad-driven gift-recipient acquisition.
// CHANGES v15.3.0: Funnel events derived from fetch responses, NOT dataLayer.
//   Purple Carrot's site doesn't push GA4 standard events (add_to_cart,
//   add_shipping_info, etc.) to window.dataLayer — those go to third-party
//   trackers (Heap, Axon, Meta Pixel). The reliable signals are the actual
//   API calls the site makes during checkout, observable via fetch intercept:
//
//     Cart      ←  POST /api/v2/carts/...?commit=true
//     Account   ←  /api/v2/users/me  returning a non-empty email
//     Shipping  ←  POST/PUT /api/v2/users/me/shipping_addresses
//     Payment   ←  Braintree tokenization (api.braintreegateway.com)
//                  or POST /api/v2/payment_methods
//
//   Each funnel event still fires once per visitor per session, with full
//   UTM/click-ID context. Purchase + order-ID gate from v15.2.0 unchanged.
// CHANGES v15.2.0: Real-order-ID gating on purchase fires.
//   ONLY confirmed Purple Carrot order IDs (numeric, 7 chars, starts with
//   297/298/299) trigger the purchase pipeline (DLVE + StackAdapt conv + SA-RT).
// CHANGES v15.1.0: Funnel-stage event capture on top of v15.0.0.
(function(){
const ADVERTISER_ID="1060";
const CONVERSION_ID="QwxIISziQhWR";
const ENDPOINT_URL="https://data.script.flowershop.media";
const STATIC_JS_URL="https://dlyyrzii9sh86.cloudfront.net/js_include/global_dlve.js?v=2.6.8";
const SAQ_PIXEL_KEY="xt5chVHtrnw0wJZcvE7Ael";
const SAQ_RT_SID="5dEaYPe2HyCbyWjEIxHtem";
const SCRIPT_VERSION="v15.4.0";
const SCRIPT_TYPE="Universal DLVE";
const UTM_STORAGE_KEY="dlve_utm_context_pc";
const UTM_COOKIE_KEY="dlve_utm_context_pc";
/* =========================================================
   UTM LANDING CONTEXT CAPTURE
   Stores UTM params + click IDs on first/each ad-driven page load.
   Persists across SPA navigation so ordersuccess + funnel stages
   always have source attribution.
========================================================= */
function storeUtmCookie(json){
  try{
    document.cookie=UTM_COOKIE_KEY+"="+encodeURIComponent(json)+
      "; path=/; max-age="+(60*60*24*30)+"; SameSite=Lax";
  }catch(e){}
}
function readUtmCookie(){
  try{
    const m=document.cookie.match(new RegExp("(?:^|; )"+UTM_COOKIE_KEY+"=([^;]*)"));
    if(!m) return null;
    return JSON.parse(decodeURIComponent(m[1]));
  }catch(e){return null;}
}
function captureUtmContext(){
  try{
    const params=new URLSearchParams(location.search);
    const ctx={};
    params.forEach((v,k)=>{
      if(k.startsWith("utm_")||k==="gclid"||k==="fbclid"||k==="msclkid"){
        ctx[k]=v;
      }
    });
    if(Object.keys(ctx).length>0){
      ctx._landing_url=location.href;
      ctx._captured_at=new Date().toISOString();
      const json=JSON.stringify(ctx);
      localStorage.setItem(UTM_STORAGE_KEY,json);
      storeUtmCookie(json);
    }
  }catch(e){}
}
function getUtmContext(){
  try{
    const s=localStorage.getItem(UTM_STORAGE_KEY);
    if(s) return JSON.parse(s);
  }catch(e){}
  return readUtmCookie()||{};
}
captureUtmContext();
(function(){
  const push=history.pushState;
  history.pushState=function(){push.apply(history,arguments);captureUtmContext();};
  const replace=history.replaceState;
  history.replaceState=function(){replace.apply(history,arguments);captureUtmContext();};
  window.addEventListener("popstate",captureUtmContext);
})();
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
    /* v15.3.0: A users/me response with an email = user has an account.
       Fires the "account" funnel stage (deduped per session). */
    processFunnelStage("account", { email: user.email, user_id: user.id, zip_code: user.zipCode });
  }catch{}
}
/* =========================================================
   PAYLOAD BUILDER (PURCHASE)
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
  const utm=getUtmContext();
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
    landing_page:cap(utm._landing_url||""),
    utm_source:utm.utm_source||"",
    utm_medium:utm.utm_medium||"",
    utm_campaign:utm.utm_campaign||"",
    utm_term:utm.utm_term||"",
    utm_content:utm.utm_content||"",
    gclid:utm.gclid||"",
    fbclid:utm.fbclid||"",
    msclkid:utm.msclkid||"",
    cart_quantity:cartQty,
    dedupe_id:String(orderId),
    revenue:revenue,
    value:revenue,
    currency:ecommerce.currency||"USD",
    products:items
  };
}
/* =========================================================
   PAYLOAD BUILDER (FUNNEL STAGE — cart, account, shipping, payment)
   Slim event with stage name, cart context, UTM source/click IDs,
   and any identity already captured this session.
========================================================= */
function buildFunnelPayload(stage, evt){
  const visitor=getVisitor();
  const ecommerce=evt?.ecommerce||{};
  const items=normalizeItems(ecommerce.items||[]);
  const value=round2(parseNum(ecommerce.value ?? evt?.value));
  const cartQty=items.reduce((s,p)=>s+p.quantity,0);
  const utm=getUtmContext();
  return {
    ts:toISO(),
    event:stage,                 // "add_to_cart" | "account" | "add_shipping_info" | "add_payment_info" | "begin_checkout"
    advertiser_id:ADVERTISER_ID,
    script_id:CONVERSION_ID,
    script_type:SCRIPT_TYPE,
    script_version:SCRIPT_VERSION,
    visitor_id:visitor.visitor_id,
    visitor_status:visitor.visitor_status,
    logged_in:loggedInStatus,
    email_hash:identity.email_hash||"",
    customer_id:identity.customer_id||"",
    zip_code:identity.zip_code||"",
    event_url:cap(location.href),
    referrer:cap(document.referrer),
    landing_page:cap(utm._landing_url||""),
    utm_source:utm.utm_source||"",
    utm_medium:utm.utm_medium||"",
    utm_campaign:utm.utm_campaign||"",
    utm_term:utm.utm_term||"",
    utm_content:utm.utm_content||"",
    gclid:utm.gclid||"",
    fbclid:utm.fbclid||"",
    msclkid:utm.msclkid||"",
    cart_quantity:cartQty,
    value:value,
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
const FUNNEL_PREFIX="dlve_funnel_"+CONVERSION_ID+"_";
function fired(id){
  try{return sessionStorage.getItem(PREFIX+id)==="1";}
  catch{return false;}
}
function mark(id){
  try{sessionStorage.setItem(PREFIX+id,"1");}
  catch{}
}
/* Funnel stage: once per visitor per session per stage. */
function funnelFired(stage){
  try{return sessionStorage.getItem(FUNNEL_PREFIX+stage)==="1";}
  catch{return false;}
}
function funnelMark(stage){
  try{sessionStorage.setItem(FUNNEL_PREFIX+stage,"1");}
  catch{}
}
/* =========================================================
   REAL ORDER-ID VALIDATION  (v15.2.0)
   Purple Carrot's confirmed-purchase IDs are 7-digit numeric values
   that start with 297, 298, or 299 (DLV-TransactionID series).
   Any other shape is a cart-save / cart-fallback / intermediate state
   and MUST NOT fire DLVE or StackAdapt — those signals would otherwise
   contaminate StackAdapt's bid optimizer with false conversions.
========================================================= */
const REAL_ORDER_PREFIXES = ["297","298","299"];
function isRealOrderId(id){
  if(!id) return false;
  const s = String(id).trim();
  if(!s) return false;
  // Must be all digits, 7 chars, prefix in allow-list
  if(!/^\d{7}$/.test(s)) return false;
  return REAL_ORDER_PREFIXES.indexOf(s.slice(0,3)) !== -1;
}
/* =========================================================
   PURCHASE PROCESSOR
   v15.2.0: Gate on isRealOrderId() BEFORE any send.
========================================================= */
async function processPurchase(evt){
  try{
    if(evt?.email && !identity.email_hash){
      await setIdentityFromEmail(evt.email);
    }
  }catch{}
  const payload=buildPayload(evt);
  const id=payload.dedupe_id;
  if(!id||fired(id)) return;

  /* ── REAL-ORDER-ID GATE (v15.2.0) ──────────────────────── */
  if(!isRealOrderId(id)){
    try{ console.debug("[DLVE] skipped non-real order id:", id); }catch{}
    return;
  }

  /* ── GIFT-REDEMPTION DETECTION (v15.4.0) ──────────────────
     If the order completes from a /redeem_signup/ URL it's a
     gift recipient claiming a code, NOT a paid purchase.
     Fire a "redeem" funnel event so the dashboard can bucket
     these separately from paid revenue.                          */
  const isRedeem = (
    String(payload.event_url||"").indexOf("/redeem_signup/") !== -1 ||
    String(payload.landing_page||"").indexOf("/redeem_signup/") !== -1 ||
    Number(payload.revenue||0) === 0
  );
  if(isRedeem){
    try{ console.debug("[DLVE] gift redemption detected, firing 'redeem' funnel:", id); }catch{}
    /* Fire the funnel event but ALSO continue to fire the purchase row —
       the attribution engine still benefits from the dedupe_id record,
       and the dashboard knows to bucket /redeem_signup/ rows separately. */
    try{ processFunnelStage("redeem", evt); }catch{}
  }

  mark(id);
  await sendDLVE(payload);
  sendSA_RT(payload);
  /* Don't send a $0 row to StackAdapt's conversion pixel — would dilute their
     CPA optimizer with non-revenue events. SA-RT (retargeting) still fires
     above so the visitor is added to the audience. */
  if(payload.revenue>0 && payload.products.length){
    sendSA({...payload,products:JSON.stringify(payload.products)});
  }
}
/* =========================================================
   FUNNEL STAGE PROCESSOR
   Fires once per visitor per stage per session. Opportunistically
   captures identity (email/customer_id/zip) if available on the event.
========================================================= */
async function processFunnelStage(stage, evt){
  try{
    const email = evt?.email || evt?.user_email || evt?.userEmail || "";
    if(email && !identity.email_hash){
      await setIdentityFromEmail(email);
    }
    const cid = evt?.user_id || evt?.userId || evt?.customer_id || evt?.customerId || "";
    if(cid && !identity.customer_id){ identity.customer_id = String(cid); }
    const zip = evt?.zip || evt?.zipCode || evt?.postal || evt?.postalCode || "";
    if(zip && !identity.zip_code){ identity.zip_code = String(zip); }
  }catch{}
  if(funnelFired(stage)) return;
  funnelMark(stage);
  const payload=buildFunnelPayload(stage, evt);
  await sendDLVE(payload);
}
/* =========================================================
   DATA LAYER LISTENER (PRIMARY SIGNALS)
   Purple Carrot confirmed:
   - account_created (has transaction ID + email)
   - ordersuccess (rich payload, primary conversion)
   - logged_in (fires every page: "No" or "YES+(Email)")
   - DLV-TransactionID (internal subscription ID, numerical,
     sequential, 7 chars, starts with "297")
========================================================= */
function mapOrderSuccessToEvent(evt){
  const txId = evt?.["DLV-TransactionID"] || evt?.transaction_id || evt?.transactionId || evt?.transactionID || evt?.order_id || evt?.orderId || evt?.id || "";
  const total = evt?.transaction_total ?? evt?.transactionTotal ?? evt?.total ?? evt?.value ?? evt?.revenue ?? "";
  const currency = evt?.currency || "USD";
  const email = evt?.email || evt?.user_email || evt?.userEmail || "";
  const zip = evt?.zip || evt?.zipCode || evt?.postal || evt?.postalCode || "";
  return {
    ecommerce:{
      transaction_id: txId,
      value: total,
      currency: currency,
      items:[{
        item_id:"purple_carrot_subscription",
        item_name:"Subscription",
        item_category:"subscription",
        price: total,
        quantity:1
      }]
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
/* =========================================================
   FUNNEL EVENT NAME MAPPING
   Maps the various dataLayer event names into 4 canonical stages
   the report cares about. Each stage fires at most once per session.
========================================================= */
const FUNNEL_MAP = {
  // Cart — user picked a plan / added something to cart
  "add_to_cart":          "add_to_cart",
  "addtocart":            "add_to_cart",
  "cart_add":             "add_to_cart",
  "cart_updated":         "add_to_cart",
  "select_plan":          "add_to_cart",
  "plan_selected":        "add_to_cart",

  // Account — user submitted email / created account
  "account_created":      "account",
  "sign_up":              "account",
  "signup":               "account",
  "user_signup":          "account",
  "email_submitted":      "account",
  "email_provided":       "account",

  // Shipping — user entered address
  "add_shipping_info":    "add_shipping_info",
  "shipping_submitted":   "add_shipping_info",
  "address_submitted":    "add_shipping_info",
  "address_provided":     "add_shipping_info",

  // Payment — user entered payment info
  "add_payment_info":     "add_payment_info",
  "payment_submitted":    "add_payment_info",
  "payment_provided":     "add_payment_info",

  // Begin checkout — bonus anchor
  "begin_checkout":       "begin_checkout",
  "checkout_started":     "begin_checkout"
};
function handleDataLayerEvent(evt){
  try{
    if(evt?.logged_in !== undefined && !loggedInStatus){
      loggedInStatus = String(evt.logged_in);
    }
    const name = String(evt?.event||"").toLowerCase();
    if(!name) return;

    /* ── Purchase (highest priority) ── */
    if(name==="ordersuccess"){
      const mapped = mapOrderSuccessToEvent(evt);
      processPurchase(mapped);
      return;
    }

    /* ── Account-created identity capture (legacy behavior) ── */
    if(name==="account_created"){
      mapAccountCreated(evt);
      /* fall through — also fires "account" funnel stage below */
    }

    /* ── Funnel stages ── */
    const stage = FUNNEL_MAP[name];
    if(stage){
      processFunnelStage(stage, evt);
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
   PURPLE CARROT API ADAPTERS — funnel + fallback purchase
   v15.3.0: each adapter fires the matching funnel-stage event
   in addition to v15.2.0's purchase-fallback behavior.
========================================================= */
function handleCart(cart){
  if(!cart?.id) return;
  const items=(cart.cartItems||[]).map(i=>({
    id:i?.id||"",
    name:i?.name||"Meal",
    price: cart?.priceSummary?.total || i?.price || 0,
    quantity:1
  }));
  const total = cart?.priceSummary?.total || 0;
  /* ── 1. Funnel "Cart" event (always fires once per session) ── */
  if((items && items.length > 0) || total > 0){
    processFunnelStage("add_to_cart", {
      ecommerce:{
        value: total,
        currency: "USD",
        items: items
      }
    });
  }
  /* ── 2. Purchase fallback (gated by isRealOrderId in v15.2.0) ── */
  if(cart?.priceSummary){
    processPurchase({
      ecommerce:{
        transaction_id: cart.id,
        value: total,
        currency:"USD",
        items
      }
    });
  }
}
/* v15.3.0: Shipping address submitted */
function handleShipping(addr){
  try{
    /* PC may return a single object or {data: [...]} or {shipping_address: {...}} */
    const a = addr?.shipping_address || addr?.data || addr || {};
    const zip = a?.zip || a?.postal_code || a?.postalCode || a?.zipCode || "";
    if(zip && !identity.zip_code){ identity.zip_code = String(zip); }
    processFunnelStage("add_shipping_info", {
      zip_code: zip || "",
      ecommerce: {}
    });
  }catch{}
}
/* v15.3.0: Payment info submitted (Braintree tokenization or PC payment_methods) */
function handlePayment(){
  processFunnelStage("add_payment_info", { ecommerce: {} });
}
/* =========================================================
   FETCH INTERCEPT (IDENTITY + FUNNEL + FALLBACK PURCHASE)
   v15.3.0: extended URL matching for shipping + payment endpoints.
========================================================= */
const origFetch=window.fetch;
window.fetch=async function(){
  const res=await origFetch.apply(this,arguments);
  try{
    const url = String(arguments[0] || "");
    const opts = arguments[1] || {};
    const method = String(opts.method || "GET").toUpperCase();
    if(url){
      /* identity + account funnel */
      if(url.includes("/api/v2/users/me")){
        res.clone().json().then(handleUser).catch(()=>{});
      }
      /* cart funnel + purchase fallback */
      if(url.includes("/api/v2/carts/") && url.includes("commit=true")){
        res.clone().json().then(handleCart).catch(()=>{});
      }
      /* shipping funnel — only on writes (POST/PUT/PATCH), not on GET reads */
      if(url.includes("shipping_addresses") &&
         (method === "POST" || method === "PUT" || method === "PATCH")){
        res.clone().json().then(handleShipping).catch(()=>handleShipping({}));
      }
      /* payment funnel — Braintree tokenization + PC's own payment endpoints */
      if(method === "POST" && (
           url.includes("api.braintreegateway.com") ||
           url.includes("/payment_methods") ||
           url.includes("/braintree/") ||
           url.includes("client_token")
         )){
        handlePayment();
      }
    }
  }catch{}
  return res;
};
/* =========================================================
   XHR INTERCEPT (v15.3.0)
   Some PC API calls go through XMLHttpRequest, not fetch.
   Mirror the same URL → funnel-stage matching for completeness.
========================================================= */
(function(){
  try{
    const _open = XMLHttpRequest.prototype.open;
    const _send = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function(method, url){
      this.__dlve_method = String(method||"GET").toUpperCase();
      this.__dlve_url = String(url||"");
      return _open.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function(){
      try{
        const xhr = this;
        xhr.addEventListener("load", function(){
          try{
            const url = xhr.__dlve_url || "";
            const method = xhr.__dlve_method || "GET";
            if(!url) return;
            let body = null;
            try{ body = JSON.parse(xhr.responseText); }catch{}
            if(url.includes("/api/v2/users/me") && body){
              handleUser(body);
            }
            if(url.includes("/api/v2/carts/") && url.includes("commit=true") && body){
              handleCart(body);
            }
            if(url.includes("shipping_addresses") &&
               (method === "POST" || method === "PUT" || method === "PATCH")){
              handleShipping(body || {});
            }
            if(method === "POST" && (
                 url.includes("api.braintreegateway.com") ||
                 url.includes("/payment_methods") ||
                 url.includes("/braintree/") ||
                 url.includes("client_token")
               )){
              handlePayment();
            }
          }catch{}
        });
      }catch{}
      return _send.apply(this, arguments);
    };
  }catch(e){}
})();
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
  console.log("[DLVE] Purple Carrot "+SCRIPT_VERSION+" initialized (gate + UTM + funnel via fetch/xhr)");
},800);
})();
