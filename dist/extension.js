"use strict";var ln=Object.create;var De=Object.defineProperty;var dn=Object.getOwnPropertyDescriptor;var un=Object.getOwnPropertyNames;var hn=Object.getPrototypeOf,pn=Object.prototype.hasOwnProperty;var oe=(u,e)=>()=>(u&&(e=u(u=0)),e);var ue=(u,e)=>()=>(e||u((e={exports:{}}).exports,e),e.exports),ie=(u,e)=>{for(var t in e)De(u,t,{get:e[t],enumerable:!0})},wt=(u,e,t,s)=>{if(e&&typeof e=="object"||typeof e=="function")for(let n of un(e))!pn.call(u,n)&&n!==t&&De(u,n,{get:()=>e[n],enumerable:!(s=dn(e,n))||s.enumerable});return u};var U=(u,e,t)=>(t=u!=null?ln(hn(u)):{},wt(e||!u||!u.__esModule?De(t,"default",{value:u,enumerable:!0}):t,u)),xn=u=>wt(De({},"__esModule",{value:!0}),u);var Ne,he,Ye=oe(()=>{"use strict";Ne=U(require("vscode")),he=class u{constructor(e){this.context=e;this._connections={};this._activeConnectionName=null;this._documentConnections=new Map;this._persistentConnections=new Map;this._keepConnectionOpen=!1;this._onDidChangeConnections=new Ne.EventEmitter;this.onDidChangeConnections=this._onDidChangeConnections.event;this._onDidChangeActiveConnection=new Ne.EventEmitter;this.onDidChangeActiveConnection=this._onDidChangeActiveConnection.event;this._onDidChangeDocumentConnection=new Ne.EventEmitter;this.onDidChangeDocumentConnection=this._onDidChangeDocumentConnection.event;this._loadingPromise=this.loadConnections()}static{this.SERVICE_NAME="netezza-vscode-connections"}static{this.ACTIVE_CONN_KEY="netezza-active-connection"}async loadConnections(){this._activeConnectionName=this.context.globalState.get(u.ACTIVE_CONN_KEY)||null;let e=await this.context.secrets.get(u.SERVICE_NAME);if(e)try{this._connections=JSON.parse(e)}catch(t){console.error("Failed to parse connections:",t),this._connections={}}else{let t=await this.context.secrets.get("netezza-vscode");if(t)try{let s=JSON.parse(t);if(s&&s.host){let n=`Default (${s.host})`;this._connections={[n]:{...s,name:n}},this._activeConnectionName=n,await this.saveConnectionsToStorage()}}catch{}}this._onDidChangeConnections.fire()}async ensureLoaded(){await this._loadingPromise}async saveConnectionsToStorage(){await this.context.secrets.store(u.SERVICE_NAME,JSON.stringify(this._connections)),this._activeConnectionName?await this.context.globalState.update(u.ACTIVE_CONN_KEY,this._activeConnectionName):await this.context.globalState.update(u.ACTIVE_CONN_KEY,void 0)}async saveConnection(e){if(await this.ensureLoaded(),!e.name)throw new Error("Connection name is required");this._connections[e.name]=e,this._activeConnectionName||await this.setActiveConnection(e.name),await this.saveConnectionsToStorage(),this._onDidChangeConnections.fire()}async deleteConnection(e){if(await this.ensureLoaded(),this._connections[e]){if(await this.closePersistentConnection(e),delete this._connections[e],this._activeConnectionName===e){let t=Object.keys(this._connections);await this.setActiveConnection(t.length>0?t[0]:null)}await this.saveConnectionsToStorage(),this._onDidChangeConnections.fire()}}async getConnections(){return await this.ensureLoaded(),Object.values(this._connections)}async getConnection(e){return await this.ensureLoaded(),this._connections[e]}async setActiveConnection(e){await this.ensureLoaded(),this._activeConnectionName=e,await this.context.globalState.update(u.ACTIVE_CONN_KEY,e),this._onDidChangeActiveConnection.fire(e)}getActiveConnectionName(){return this._activeConnectionName}async getConnectionString(e){await this.ensureLoaded();let t=e||this._activeConnectionName;if(!t)return null;let s=this._connections[t];if(!s)return console.error(`[ConnectionManager] Connection '${t}' not found in registry. Available keys: ${Object.keys(this._connections).join(", ")}`),null;let n=s.dbType||"NetezzaSQL";return n==="NetezzaSQL"?`DRIVER={NetezzaSQL};SERVER=${s.host};PORT=${s.port};DATABASE=${s.database};UID=${s.user};PWD=${s.password};`:`DRIVER={${n}};SERVER=${s.host};PORT=${s.port};DATABASE=${s.database};UID=${s.user};PWD=${s.password};`}async getCurrentDatabase(e){await this.ensureLoaded();let t=e||this._activeConnectionName;return t&&this._connections[t]?.database||null}setKeepConnectionOpen(e){this._keepConnectionOpen=e,e||this.closeAllPersistentConnections()}getKeepConnectionOpen(){return this._keepConnectionOpen}async getPersistentConnection(e){let t=e||this._activeConnectionName;if(!t)throw new Error("No connection selected");let s=await this.getConnectionString(t);if(!s)throw new Error(`Connection '${t}' not found or invalid`);let n=this._persistentConnections.get(t);return n||(n=await require("odbc").connect({connectionString:s,fetchArray:!0}),this._persistentConnections.set(t,n)),n}async closePersistentConnection(e){let t=this._persistentConnections.get(e);if(t){try{await t.close()}catch(s){console.error(`Error closing connection ${e}:`,s)}this._persistentConnections.delete(e)}}async closeAllPersistentConnections(){for(let e of this._persistentConnections.keys())await this.closePersistentConnection(e)}dispose(){this.closeAllPersistentConnections()}getDocumentConnection(e){return this._documentConnections.get(e)}setDocumentConnection(e,t){this._documentConnections.set(e,t),this._onDidChangeDocumentConnection.fire(e)}clearDocumentConnection(e){this._documentConnections.delete(e),this._onDidChangeDocumentConnection.fire(e)}getConnectionForExecution(e){if(e){let t=this._documentConnections.get(e);if(t)return t}return this._activeConnectionName||void 0}}});var Et={};ie(Et,{QueryHistoryManager:()=>G});var pe,Me,G,_e=oe(()=>{"use strict";pe=U(require("fs")),Me=U(require("path")),G=class u{constructor(e){this.context=e;this.cache=[];this.initialized=!1;this.initialize()}static{this.STORAGE_KEY="queryHistory"}static{this.MAX_ENTRIES=5e4}static{this.CLEANUP_KEEP=4e4}static{this.STORAGE_VERSION=1}async initialize(){try{let e=this.context.globalState.get(u.STORAGE_KEY);e&&e.entries?(this.cache=e.entries,console.log(`\u2705 Loaded ${this.cache.length} entries from VS Code storage`)):await this.migrateFromLegacyStorage(),this.initialized=!0}catch(e){console.error("\u274C Error initializing query history:",e),this.cache=[],this.initialized=!0}}async migrateFromLegacyStorage(){try{let e=this.context.globalStorageUri.fsPath,t=Me.join(e,"query-history.db");pe.existsSync(t)&&(console.log("\u26A0\uFE0F SQLite database found but migration not implemented"),console.log("\u{1F4A1} Consider manually exporting data before switching"));let s=Me.join(e,"query-history.json");if(pe.existsSync(s)){let a=pe.readFileSync(s,"utf8");if(a.trim()){let r=JSON.parse(a);this.cache=r,await this.saveToStorage(),console.log(`\u2705 Migrated ${r.length} entries from JSON`)}}let n=Me.join(e,"query-history-archive.json");if(pe.existsSync(n)){let a=pe.readFileSync(n,"utf8");if(a.trim()){let r=JSON.parse(a);this.cache.push(...r),await this.saveToStorage(),console.log(`\u2705 Migrated archive with ${r.length} entries`)}}}catch(e){console.error("Error during migration:",e)}}async saveToStorage(){try{let e={entries:this.cache,version:u.STORAGE_VERSION};await this.context.globalState.update(u.STORAGE_KEY,e)}catch(e){console.error("Error saving to storage:",e)}}async addEntry(e,t,s,n,a,r,i){try{this.initialized||await this.initialize();let o=`${Date.now()}-${Math.random().toString(36).substring(2,9)}`,h=Date.now(),c={id:o,host:e,database:t,schema:s,query:n.trim(),timestamp:h,connectionName:a,is_favorite:!1,tags:r||"",description:i||""};this.cache.unshift(c),this.cache.length>u.MAX_ENTRIES&&(this.cache=this.cache.slice(0,u.CLEANUP_KEEP),console.log(`Cleaned up old entries, keeping ${u.CLEANUP_KEEP} newest`)),await this.saveToStorage()}catch(o){console.error("Error adding query to history:",o)}}async getHistory(){return this.initialized||await this.initialize(),[...this.cache]}async deleteEntry(e){try{this.cache=this.cache.filter(t=>t.id!==e),await this.saveToStorage()}catch(t){console.error("Error deleting entry:",t)}}async clearHistory(){try{this.cache=[],await this.saveToStorage(),console.log("All query history cleared")}catch(e){console.error("Error clearing history:",e)}}async getStats(){try{let e=this.cache.length,t=JSON.stringify(this.cache).length,s=parseFloat((t/(1024*1024)).toFixed(2));return{activeEntries:e,archivedEntries:0,totalEntries:e,activeFileSizeMB:s,archiveFileSizeMB:0,totalFileSizeMB:s}}catch(e){return console.error("Error getting stats:",e),{activeEntries:0,archivedEntries:0,totalEntries:0,activeFileSizeMB:0,archiveFileSizeMB:0,totalFileSizeMB:0}}}async toggleFavorite(e){try{let t=this.cache.find(s=>s.id===e);t&&(t.is_favorite=!t.is_favorite,await this.saveToStorage())}catch(t){console.error("Error toggling favorite:",t)}}async updateEntry(e,t,s){try{let n=this.cache.find(a=>a.id===e);n&&(t!==void 0&&(n.tags=t),s!==void 0&&(n.description=s),await this.saveToStorage())}catch(n){console.error("Error updating entry:",n)}}async getFavorites(){return this.initialized||await this.initialize(),this.cache.filter(e=>e.is_favorite)}async getByTag(e){return this.initialized||await this.initialize(),this.cache.filter(t=>t.tags?.toLowerCase().includes(e.toLowerCase()))}async getAllTags(){this.initialized||await this.initialize();let e=new Set;return this.cache.forEach(t=>{t.tags&&t.tags.split(",").forEach(n=>{let a=n.trim();a&&e.add(a)})}),Array.from(e).sort()}async searchAll(e){this.initialized||await this.initialize();let t=e.toLowerCase();return this.cache.filter(s=>s.query.toLowerCase().includes(t)||s.host.toLowerCase().includes(t)||s.database.toLowerCase().includes(t)||s.schema.toLowerCase().includes(t)||s.tags?.toLowerCase().includes(t)||s.description?.toLowerCase().includes(t))}async getFilteredHistory(e,t,s,n){this.initialized||await this.initialize();let a=this.cache.filter(r=>!(e&&r.host!==e||t&&r.database!==t||s&&r.schema!==s));return n&&(a=a.slice(0,n)),a}async getArchivedHistory(){return[]}async clearArchive(){}close(){console.log("Query history manager closed")}}});var It={};ie(It,{clearCurrentSession:()=>$e,getCurrentSessionConnectionName:()=>Ge,getCurrentSessionId:()=>Ke,runQueriesSequentially:()=>Ze,runQuery:()=>F,runQueryRaw:()=>St});function Ke(){return Oe}function Ge(){return Qe}function $e(){Oe=null,Qe=void 0}function yt(u){let e=u.match(/SERVER=([^;]+)/i),t=u.match(/DATABASE=([^;]+)/i);return{host:e?e[1]:"unknown",database:t?t[1]:"unknown"}}function vt(u){let e=new Set;if(!u)return e;for(let t of u.matchAll(/\$\{([A-Za-z0-9_]+)\}/g))t[1]&&e.add(t[1]);return e}function Ct(u){if(!u)return{sql:"",setValues:{}};let e=u.split(/\r?\n/),t=[],s={};for(let n of e){let a=n.match(/^\s*@SET\s+([A-Za-z0-9_]+)\s*=\s*(.+)$/i);if(a){let r=a[2].trim();r.endsWith(";")&&(r=r.slice(0,-1).trim());let i=r.match(/^'(.*)'$/s)||r.match(/^"(.*)"$/s);i&&(r=i[1]),s[a[1]]=r}else t.push(n)}return{sql:t.join(`
`),setValues:s}}async function bt(u,e,t){let s={};if(u.size===0)return s;if(e){let a=Array.from(u).filter(r=>!(t&&t[r]!==void 0));if(a.length>0)throw new Error("Query contains variables but silent mode is enabled; cannot prompt for values. Missing: "+a.join(", "));for(let r of u)s[r]=t[r];return s}let n=[];for(let a of u)t&&t[a]!==void 0?s[a]=t[a]:n.push(a);for(let a of n){let r=await xe.window.showInputBox({prompt:`Enter value for ${a}`,placeHolder:"",value:t&&t[a]?t[a]:void 0,ignoreFocusOut:!0});if(r===void 0)throw new Error("Variable input cancelled by user");s[a]=r}return s}function Tt(u,e){return u.replace(/\$\{([A-Za-z0-9_]+)\}/g,(t,s)=>e[s]??"")}async function St(u,e,t=!1,s,n,a){if(!be)throw new Error("odbc package not installed. Please run: npm install odbc");let r=s||new he(u),i=r.getKeepConnectionOpen(),o;t||(o=xe.window.createOutputChannel("Netezza SQL"),o.show(!0),o.appendLine("Executing query..."),n&&o.appendLine(`Target Connection: ${n}`));try{let h=Ct(e),c=h.sql,x=h.setValues,p=vt(c);if(p.size>0){let A=await bt(p,t,x);c=Tt(c,A)}let m,g=!0,y,w=n;if(!w&&a&&(w=r.getConnectionForExecution(a)),w||(w=r.getActiveConnectionName()||void 0),i){m=await r.getPersistentConnection(w),g=!1;let A=await r.getConnectionString(w);if(!A)throw new Error("Connection not configured. Please connect via Netezza: Connect...");y=A}else{let A=await r.getConnectionString(w);if(!A)throw new Error("Connection not configured. Please connect via Netezza: Connect...");y=A,m=await be.connect({connectionString:y,fetchArray:!0})}try{let M=xe.workspace.getConfiguration("netezza").get("queryTimeout",1800),D=await At(m,c,2e5,M),L="unknown";try{let k=await m.query("SELECT CURRENT_SCHEMA");k&&k.length>0&&(Array.isArray(k[0])?L=k[0][0]||"unknown":L=k[0].CURRENT_SCHEMA||"unknown")}catch(k){console.debug("Could not retrieve current schema:",k)}let z=yt(y);if(new G(u).addEntry(z.host,z.database,L,e,w).catch(k=>{console.error("Failed to log query to history:",k)}),D&&Array.isArray(D)){let k=D.columns?D.columns.map(H=>({name:H.name,type:H.dataType})):[];return o&&o.appendLine("Query completed."),{columns:k,data:D,rowsAffected:D.count,limitReached:D.limitReached,sql:c}}else return o&&o.appendLine("Query executed successfully (no results)."),{columns:[],data:[],rowsAffected:D?.count,message:"Query executed successfully (no results).",sql:c}}finally{g&&await m.close()}}catch(h){let c=je(h);throw o&&o.appendLine(c),new Error(c)}}async function F(u,e,t=!1,s,n,a){try{let r=await St(u,e,t,n,s,a);if(r.data&&r.data.length>0){let i=r.data.map(h=>{let c={};return r.columns.forEach((x,p)=>{c[x.name]=h[p]}),c});return JSON.stringify(i,(h,c)=>typeof c=="bigint"?c>=Number.MIN_SAFE_INTEGER&&c<=Number.MAX_SAFE_INTEGER?Number(c):c.toString():c,2)}else if(r.message)return r.message;return}catch(r){throw r}}async function Ze(u,e,t,s){if(!be)throw new Error("odbc package not installed. Please run: npm install odbc");let n=t||new he(u),a=n.getKeepConnectionOpen(),r=xe.window.createOutputChannel("Netezza SQL");r.show(!0),r.appendLine(`Executing ${e.length} queries sequentially...`);let i=[],o=t?t.getConnectionForExecution(s):void 0;o||(o=n.getActiveConnectionName()||void 0);try{let h,c=!0,x;if(a){h=await n.getPersistentConnection(o),c=!1;let p=await n.getConnectionString(o);if(!p)throw new Error("Connection not configured. Please connect via Netezza: Connect...");x=p}else{let p=await n.getConnectionString(o);if(!p)throw new Error("Connection not configured. Please connect via Netezza: Connect...");x=p,h=await be.connect({connectionString:x,fetchArray:!0})}try{let p="unknown";try{let w=await h.query("SELECT CURRENT_SCHEMA");w&&w.length>0&&(Array.isArray(w[0])?p=w[0][0]||"unknown":p=w[0].CURRENT_SCHEMA||"unknown")}catch(w){console.debug("Could not retrieve current schema:",w)}try{let w=await h.query("SELECT CURRENT_SID");w&&w.length>0&&(Oe=Array.isArray(w[0])?w[0][0]:w[0].CURRENT_SID,Qe=o,r.appendLine(`Session ID: ${Oe}`))}catch(w){console.debug("Could not retrieve session ID:",w)}let m=yt(x),g=new G(u),y=o;for(let w=0;w<e.length;w++){let A=e[w];r.appendLine(`Executing query ${w+1}/${e.length}...`);try{let M=Ct(A),D=M.sql,L=M.setValues,z=vt(D);if(z.size>0){let Y=await bt(z,!1,L);D=Tt(D,Y)}let k=xe.workspace.getConfiguration("netezza").get("queryTimeout",1800),H=await At(h,D,2e5,k);if(g.addEntry(m.host,m.database,p,A,y).catch(Y=>{console.error("Failed to log query to history:",Y)}),H&&Array.isArray(H)){let Y=H.columns?H.columns.map(ne=>({name:ne.name,type:ne.dataType})):[];i.push({columns:Y,data:H,rowsAffected:H.count,limitReached:H.limitReached,sql:D})}else i.push({columns:[],data:[],rowsAffected:H?.count,message:"Query executed successfully",sql:D})}catch(M){let D=je(M);throw r.appendLine(`Error in query ${w+1}: ${D}`),new Error(D)}}r.appendLine("All queries completed.")}finally{$e(),c&&await h.close()}}catch(h){let c=je(h);throw r.appendLine(c),new Error(c)}return i}function je(u){return u.odbcErrors&&Array.isArray(u.odbcErrors)&&u.odbcErrors.length>0?u.odbcErrors.map(e=>`[ODBC Error] State: ${e.state}, Native Code: ${e.code}
Message: ${e.message}`).join(`

`):`Error: ${u.message||u}`}async function At(u,e,t,s){let n=await u.createStatement();try{await n.prepare(e);let a={cursor:!0,fetchSize:5e3};s&&s>0&&(a.timeout=s);let r=await n.execute(a),i=[],o=0,h=await r.fetch(),c=h?h.columns:void 0,x=h.count;for(;h&&h.length>0;){!c&&h.columns&&(c=h.columns);for(let p of h)if(i.push(p),o++,o>=t){i.limitReached=!0;break}if(o>=t){i.limitReached=!0;break}h=await r.fetch()}return await r.close(),c&&(i.columns=c),i.count=x,i}catch(a){throw a}finally{await n.close()}}var xe,be,Oe,Qe,fe=oe(()=>{"use strict";xe=U(require("vscode"));Ye();_e();try{be=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}Oe=null});var et={};ie(et,{SchemaItem:()=>ee,SchemaProvider:()=>Te});var P,Te,ee,Pe=oe(()=>{"use strict";P=U(require("vscode"));fe();Te=class{constructor(e,t,s){this.context=e;this.connectionManager=t;this.metadataCache=s;this._onDidChangeTreeData=new P.EventEmitter;this.onDidChangeTreeData=this._onDidChangeTreeData.event;this.connectionManager.onDidChangeConnections(()=>this.refresh())}refresh(){this._onDidChangeTreeData.fire()}getTreeItem(e){return e}getParent(e){if(e.contextValue!=="serverInstance"){if(e.contextValue==="database")return new ee(e.connectionName,P.TreeItemCollapsibleState.Collapsed,"serverInstance",void 0,void 0,void 0,void 0,void 0,e.connectionName,void 0);if(e.contextValue==="typeGroup")return new ee(e.dbName,P.TreeItemCollapsibleState.Collapsed,"database",e.dbName,void 0,void 0,void 0,void 0,e.connectionName);if(e.contextValue.startsWith("netezza:"))return new ee(e.objType,P.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,e.objType,void 0,void 0,void 0,e.connectionName)}}async getChildren(e){if(e){if(e.contextValue==="serverInstance"){if(!e.connectionName)return[];let t=this.metadataCache.getDatabases(e.connectionName);if(t)return t.map(s=>new ee(s.label||s.DATABASE,P.TreeItemCollapsibleState.Collapsed,"database",s.label||s.DATABASE,void 0,void 0,void 0,void 0,e.connectionName));try{let s=await F(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0,e.connectionName,this.connectionManager);if(!s)return[];let n=JSON.parse(s),a=n.map(r=>({label:r.DATABASE,kind:9,detail:"Database"}));return this.metadataCache.setDatabases(e.connectionName,a),n.map(r=>new ee(r.DATABASE,P.TreeItemCollapsibleState.Collapsed,"database",r.DATABASE,void 0,void 0,void 0,void 0,e.connectionName))}catch(s){return P.window.showErrorMessage(`Failed to load databases for ${e.connectionName}: ${s}`),[]}}else if(e.contextValue==="database")try{let t=`SELECT DISTINCT OBJTYPE FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' ORDER BY OBJTYPE`,s=await F(this.context,t,!0,e.connectionName,this.connectionManager);return JSON.parse(s||"[]").map(a=>new ee(a.OBJTYPE,P.TreeItemCollapsibleState.Collapsed,"typeGroup",e.dbName,a.OBJTYPE,void 0,void 0,void 0,e.connectionName))}catch(t){return P.window.showErrorMessage("Failed to load object types: "+t),[]}else if(e.contextValue==="typeGroup")try{let t=`SELECT OBJNAME, SCHEMA, OBJID, COALESCE(DESCRIPTION, '') AS DESCRIPTION FROM ${e.dbName}.._V_OBJECT_DATA WHERE DBNAME = '${e.dbName}' AND OBJTYPE = '${e.objType}' ORDER BY OBJNAME`,s=await F(this.context,t,!0,e.connectionName,this.connectionManager),n=JSON.parse(s||"[]");if(e.connectionName&&e.dbName&&(e.objType==="TABLE"||e.objType==="VIEW")){let a=new Map;for(let r of n){let i=r.SCHEMA?`${e.dbName}.${r.SCHEMA}`:`${e.dbName}..`;a.has(i)||a.set(i,{tables:[],idMap:new Map});let o=a.get(i);o.tables.push({label:r.OBJNAME,kind:e.objType==="VIEW"?18:7,detail:r.SCHEMA?e.objType:`${e.objType} (${r.SCHEMA})`,sortText:r.OBJNAME});let h=r.SCHEMA?`${e.dbName}.${r.SCHEMA}.${r.OBJNAME}`:`${e.dbName}..${r.OBJNAME}`;o.idMap.set(h,r.OBJID)}for(let[r,i]of a);}return n.map(a=>{let i=["TABLE","VIEW","EXTERNAL TABLE","SYSTEM VIEW","SYSTEM TABLE"].includes(e.objType||"");return new ee(a.OBJNAME,i?P.TreeItemCollapsibleState.Collapsed:P.TreeItemCollapsibleState.None,`netezza:${e.objType}`,e.dbName,e.objType,a.SCHEMA,a.OBJID,a.DESCRIPTION,e.connectionName)})}catch(t){return P.window.showErrorMessage("Failed to load objects: "+t),[]}else if(e.contextValue.startsWith("netezza:")&&e.objId){let t=e.label,s=e.schema,n=e.dbName;if(e.connectionName&&n){let a=`${n}.${s||""}.${t}`,r=this.metadataCache.getColumns(e.connectionName,a);if(r)return r.map(i=>new ee(i.detail?`${i.label} (${i.detail})`:i.label,P.TreeItemCollapsibleState.None,"column",e.dbName,void 0,void 0,void 0,i.documentation||"",e.connectionName))}try{let a=`SELECT 
                        X.ATTNAME
                        , X.FORMAT_TYPE
                        , X.ATTNOTNULL::BOOL AS ATTNOTNULL
                        , X.COLDEFAULT
                        , COALESCE(X.DESCRIPTION, '') AS DESCRIPTION
                    FROM
                        ${e.dbName}.._V_RELATION_COLUMN X
                    WHERE
                        X.OBJID = ${e.objId}
                    ORDER BY 
                        X.ATTNUM`,r=await F(this.context,a,!0,e.connectionName,this.connectionManager),i=JSON.parse(r||"[]");if(e.connectionName&&n){let o=`${n}.${s||""}.${t}`,h=i.map(c=>({label:c.ATTNAME,kind:5,detail:c.FORMAT_TYPE,documentation:c.DESCRIPTION}));this.metadataCache.setColumns(e.connectionName,o,h)}return i.map(o=>new ee(`${o.ATTNAME} (${o.FORMAT_TYPE})`,P.TreeItemCollapsibleState.None,"column",e.dbName,void 0,void 0,void 0,o.DESCRIPTION,e.connectionName))}catch(a){return P.window.showErrorMessage("Failed to load columns: "+a),[]}}}else{let t=await this.connectionManager.getConnections(),s=P.Uri.file(this.context.asAbsolutePath("netezza_icon64.png"));return t.map(n=>new ee(n.name,P.TreeItemCollapsibleState.Collapsed,"serverInstance",void 0,void 0,void 0,void 0,void 0,n.name,s))}return[]}},ee=class extends P.TreeItem{constructor(t,s,n,a,r,i,o,h,c,x){super(t,s);this.label=t;this.collapsibleState=s;this.contextValue=n;this.dbName=a;this.objType=r;this.schema=i;this.objId=o;this.objectDescription=h;this.connectionName=c;let p=this.label;c&&(p+=`
[Server: ${c}]`),h&&h.trim()&&(p+=`

${h.trim()}`),i&&n.startsWith("netezza:")&&(p+=`

Schema: ${i}`),this.tooltip=p,this.description=i?`(${i})`:"",x?this.iconPath=x:n==="serverInstance"?this.iconPath=new P.ThemeIcon("server"):n==="database"?this.iconPath=new P.ThemeIcon("database"):n==="typeGroup"?this.iconPath=new P.ThemeIcon("folder"):n.startsWith("netezza:")?this.iconPath=this.getIconForType(r):n==="column"&&(this.iconPath=new P.ThemeIcon("symbol-field"))}getIconForType(t){switch(t){case"TABLE":return new P.ThemeIcon("table");case"VIEW":return new P.ThemeIcon("eye");case"PROCEDURE":return new P.ThemeIcon("gear");case"FUNCTION":return new P.ThemeIcon("symbol-function");case"AGGREGATE":return new P.ThemeIcon("symbol-operator");case"EXTERNAL TABLE":return new P.ThemeIcon("server");default:return new P.ThemeIcon("file")}}}});var Nt={};ie(Nt,{generateDDL:()=>Sn});function q(u){return!u||/^[A-Z_][A-Z0-9_]*$/i.test(u)&&u===u.toUpperCase()?u:`"${u.replace(/"/g,'""')}"`}async function Dt(u,e,t,s){let n=`
        SELECT 
            X.OBJID::INT AS OBJID
            , X.ATTNAME
            , X.DESCRIPTION
            , X.FORMAT_TYPE AS FULL_TYPE
            , X.ATTNOTNULL::BOOL AS ATTNOTNULL
            , X.COLDEFAULT
        FROM
            ${e.toUpperCase()}.._V_RELATION_COLUMN X
        INNER JOIN
            ${e.toUpperCase()}.._V_OBJECT_DATA D ON X.OBJID = D.OBJID
        WHERE
            X.TYPE IN ('TABLE','VIEW','EXTERNAL TABLE', 'SEQUENCE','SYSTEM VIEW','SYSTEM TABLE')
            AND X.OBJID NOT IN (4,5)
            AND D.SCHEMA = '${t.toUpperCase()}'
            AND D.OBJNAME = '${s.toUpperCase()}'
        ORDER BY 
            X.OBJID, X.ATTNUM
    `,a=await u.query(n),r=[];for(let i of a){let o=!1,h=i.ATTNOTNULL;if(typeof h=="boolean")o=h;else if(typeof h=="number")o=h!==0;else if(typeof h=="string"){let c=h.trim().toLowerCase();o=c==="t"||c==="true"||c==="1"||c==="yes"}r.push({name:i.ATTNAME,description:i.DESCRIPTION||null,fullTypeName:i.FULL_TYPE,notNull:o,defaultValue:i.COLDEFAULT||null})}return r}async function gn(u,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_DIST_MAP
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY DISTSEQNO
        `;return(await u.query(n)).map(r=>r.ATTNAME)}catch{return[]}}async function fn(u,e,t,s){try{let n=`
            SELECT ATTNAME
            FROM ${e.toUpperCase()}.._V_TABLE_ORGANIZE_COLUMN
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND TABLENAME = '${s.toUpperCase()}'
            ORDER BY ORGSEQNO
        `;return(await u.query(n)).map(r=>r.ATTNAME)}catch{return[]}}async function wn(u,e,t,s){let n=`
        SELECT 
            X.SCHEMA
            , X.RELATION
            , X.CONSTRAINTNAME
            , X.CONTYPE
            , X.ATTNAME
            , X.PKDATABASE
            , X.PKSCHEMA
            , X.PKRELATION
            , X.PKATTNAME
            , X.UPDT_TYPE
            , X.DEL_TYPE
        FROM 
            ${e.toUpperCase()}.._V_RELATION_KEYDATA X
        WHERE 
            X.OBJID NOT IN (4,5)
            AND X.SCHEMA = '${t.toUpperCase()}'
            AND X.RELATION = '${s.toUpperCase()}'
        ORDER BY
            X.SCHEMA, X.RELATION, X.CONSEQ
    `,a=new Map;try{let r=await u.query(n);for(let i of r){let o=i.CONSTRAINTNAME;if(!a.has(o)){let c={p:"PRIMARY KEY",f:"FOREIGN KEY",u:"UNIQUE"};a.set(o,{type:c[i.CONTYPE]||"UNKNOWN",typeChar:i.CONTYPE,columns:[],pkDatabase:i.PKDATABASE||null,pkSchema:i.PKSCHEMA||null,pkRelation:i.PKRELATION||null,pkColumns:[],updateType:i.UPDT_TYPE||"NO ACTION",deleteType:i.DEL_TYPE||"NO ACTION"})}let h=a.get(o);h.columns.push(i.ATTNAME),i.PKATTNAME&&h.pkColumns.push(i.PKATTNAME)}}catch(r){console.warn("Cannot retrieve keys info:",r)}return a}async function En(u,e,t,s){try{let n=`
            SELECT DESCRIPTION
            FROM ${e.toUpperCase()}.._V_OBJECT_DATA
            WHERE SCHEMA = '${t.toUpperCase()}'
                AND OBJNAME = '${s.toUpperCase()}'
                AND OBJTYPE = 'TABLE'
        `,a=await u.query(n);if(a.length>0&&a[0].DESCRIPTION)return a[0].DESCRIPTION}catch{try{let n=`
                SELECT DESCRIPTION
                FROM ${e.toUpperCase()}.._V_OBJECT_DATA
                WHERE SCHEMA = '${t.toUpperCase()}'
                    AND OBJNAME = '${s.toUpperCase()}'
            `,a=await u.query(n);if(a.length>0&&a[0].DESCRIPTION)return a[0].DESCRIPTION}catch{}}return null}async function yn(u,e,t,s){let n=await Dt(u,e,t,s);if(n.length===0)throw new Error(`Table ${e}.${t}.${s} not found or has no columns`);let a=await gn(u,e,t,s),r=await fn(u,e,t,s),i=await wn(u,e,t,s),o=await En(u,e,t,s),h=q(e),c=q(t),x=q(s),p=[];p.push(`CREATE TABLE ${h}.${c}.${x}`),p.push("(");let m=[];for(let g of n){let w=`    ${q(g.name)} ${g.fullTypeName}`;g.notNull&&(w+=" NOT NULL"),g.defaultValue!==null&&(w+=` DEFAULT ${g.defaultValue}`),m.push(w)}if(p.push(m.join(`,
`)),a.length>0){let g=a.map(y=>q(y));p.push(`)
DISTRIBUTE ON (${g.join(", ")})`)}else p.push(`)
DISTRIBUTE ON RANDOM`);if(r.length>0){let g=r.map(y=>q(y));p.push(`ORGANIZE ON (${g.join(", ")})`)}p.push(";"),p.push("");for(let[g,y]of i){let w=q(g),A=y.columns.map(M=>q(M));if(y.typeChar==="f"){let M=y.pkColumns.filter(D=>D).map(D=>q(D));M.length>0&&p.push(`ALTER TABLE ${h}.${c}.${x} ADD CONSTRAINT ${w} ${y.type} (${A.join(", ")}) REFERENCES ${y.pkDatabase}.${y.pkSchema}.${y.pkRelation} (${M.join(", ")}) ON DELETE ${y.deleteType} ON UPDATE ${y.updateType};`)}else(y.typeChar==="p"||y.typeChar==="u")&&p.push(`ALTER TABLE ${h}.${c}.${x} ADD CONSTRAINT ${w} ${y.type} (${A.join(", ")});`)}if(o){let g=o.replace(/'/g,"''");p.push(""),p.push(`COMMENT ON TABLE ${h}.${c}.${x} IS '${g}';`)}for(let g of n)if(g.description){let y=q(g.name),w=g.description.replace(/'/g,"''");p.push(`COMMENT ON COLUMN ${h}.${c}.${x}.${y} IS '${w}';`)}return p.join(`
`)}async function vn(u,e,t,s){let n=`
        SELECT 
            SCHEMA,
            VIEWNAME,
            DEFINITION,
            OBJID::INT
        FROM ${e.toUpperCase()}.._V_VIEW
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND VIEWNAME = '${s.toUpperCase()}'
    `,r=await u.query(n);if(r.length===0)throw new Error(`View ${e}.${t}.${s} not found`);let i=r[0],o=q(e),h=q(t),c=q(s),x=[];return x.push(`CREATE OR REPLACE VIEW ${o}.${h}.${c} AS`),x.push(i.DEFINITION||""),x.join(`
`)}async function Cn(u,e,t,s){let n=`
        SELECT 
            SCHEMA,
            PROCEDURESOURCE,
            OBJID::INT,
            RETURNS,
            EXECUTEDASOWNER,
            DESCRIPTION,
            PROCEDURESIGNATURE,
            ARGUMENTS,
            NULL AS LANGUAGE
        FROM ${e.toUpperCase()}.._V_PROCEDURE
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND PROCEDURE = '${s.toUpperCase()}'
        ORDER BY 1, 2, 3
    `,r=await u.query(n);if(r.length===0)throw new Error(`Procedure ${e}.${t}.${s} not found`);let i=r[0],o={schema:i.SCHEMA,procedureSource:i.PROCEDURESOURCE,objId:i.OBJID,returns:i.RETURNS,executeAsOwner:!!i.EXECUTEDASOWNER,description:i.DESCRIPTION||null,procedureSignature:i.PROCEDURESIGNATURE,arguments:i.ARGUMENTS||null},h=q(e),c=q(t),x=q(s),p=[];if(p.push(`CREATE OR REPLACE PROCEDURE ${h}.${c}.${x}`),p.push(`RETURNS ${o.returns}`),o.executeAsOwner?p.push("EXECUTE AS OWNER"):p.push("EXECUTE AS CALLER"),p.push("LANGUAGE NZPLSQL AS"),p.push("BEGIN_PROC"),p.push(o.procedureSource),p.push("END_PROC;"),o.description){let m=o.description.replace(/'/g,"''");p.push(`COMMENT ON PROCEDURE ${x} IS '${m}';`)}return p.join(`
`)}async function bn(u,e,t,s){let n=`
        SELECT 
            E1.SCHEMA,
            E1.TABLENAME,
            E2.EXTOBJNAME,
            E2.OBJID::INT,
            E1.DELIM,
            E1.ENCODING,
            E1.TIMESTYLE,
            E1.REMOTESOURCE,
            E1.SKIPROWS,
            E1.MAXERRORS,
            E1.ESCAPE,
            E1.LOGDIR,
            E1.DECIMALDELIM,
            E1.QUOTEDVALUE,
            E1.NULLVALUE,
            E1.CRINSTRING,
            E1.TRUNCSTRING,
            E1.CTRLCHARS,
            E1.IGNOREZERO,
            E1.TIMEEXTRAZEROS,
            E1.Y2BASE,
            E1.FILLRECORD,
            E1.COMPRESS,
            E1.INCLUDEHEADER,
            E1.LFINSTRING,
            E1.DATESTYLE,
            E1.DATEDELIM,
            E1.TIMEDELIM,
            E1.BOOLSTYLE,
            E1.FORMAT,
            E1.SOCKETBUFSIZE,
            E1.RECORDDELIM,
            E1.MAXROWS,
            E1.REQUIREQUOTES,
            E1.RECORDLENGTH,
            E1.DATETIMEDELIM,
            E1.REJECTFILE
        FROM 
            ${e.toUpperCase()}.._V_EXTERNAL E1
            JOIN ${e.toUpperCase()}.._V_EXTOBJECT E2 ON E1.DATABASE = E2.DATABASE
                AND E1.SCHEMA = E2.SCHEMA
                AND E1.TABLENAME = E2.TABLENAME
        WHERE 
            E1.DATABASE = '${e.toUpperCase()}'
            AND E1.SCHEMA = '${t.toUpperCase()}'
            AND E1.TABLENAME = '${s.toUpperCase()}'
    `,r=await u.query(n);if(r.length===0)throw new Error(`External table ${e}.${t}.${s} not found`);let i=r[0],o={schema:i.SCHEMA,tableName:i.TABLENAME,dataObject:i.EXTOBJNAME||null,delimiter:i.DELIM||null,encoding:i.ENCODING||null,timeStyle:i.TIMESTYLE||null,remoteSource:i.REMOTESOURCE||null,skipRows:i.SKIPROWS||null,maxErrors:i.MAXERRORS||null,escapeChar:i.ESCAPE||null,logDir:i.LOGDIR||null,decimalDelim:i.DECIMALDELIM||null,quotedValue:i.QUOTEDVALUE||null,nullValue:i.NULLVALUE||null,crInString:i.CRINSTRING??null,truncString:i.TRUNCSTRING??null,ctrlChars:i.CTRLCHARS??null,ignoreZero:i.IGNOREZERO??null,timeExtraZeros:i.TIMEEXTRAZEROS??null,y2Base:i.Y2BASE||null,fillRecord:i.FILLRECORD??null,compress:i.COMPRESS||null,includeHeader:i.INCLUDEHEADER??null,lfInString:i.LFINSTRING??null,dateStyle:i.DATESTYLE||null,dateDelim:i.DATEDELIM||null,timeDelim:i.TIMEDELIM||null,boolStyle:i.BOOLSTYLE||null,format:i.FORMAT||null,socketBufSize:i.SOCKETBUFSIZE||null,recordDelim:i.RECORDDELIM?String(i.RECORDDELIM).replace(/\r/g,"\\r").replace(/\n/g,"\\n"):null,maxRows:i.MAXROWS||null,requireQuotes:i.REQUIREQUOTES??null,recordLength:i.RECORDLENGTH||null,dateTimeDelim:i.DATETIMEDELIM||null,rejectFile:i.REJECTFILE||null},h=await Dt(u,e,t,s),c=q(e),x=q(t),p=q(s),m=[];m.push(`CREATE EXTERNAL TABLE ${c}.${x}.${p}`),m.push("(");let g=h.map(y=>{let w=`    ${q(y.name)} ${y.fullTypeName}`;return y.notNull&&(w+=" NOT NULL"),w});return m.push(g.join(`,
`)),m.push(")"),m.push("USING"),m.push("("),o.dataObject!==null&&m.push(`    DATAOBJECT('${o.dataObject}')`),o.delimiter!==null&&m.push(`    DELIMITER '${o.delimiter}'`),o.encoding!==null&&m.push(`    ENCODING '${o.encoding}'`),o.timeStyle!==null&&m.push(`    TIMESTYLE '${o.timeStyle}'`),o.remoteSource!==null&&m.push(`    REMOTESOURCE '${o.remoteSource}'`),o.maxErrors!==null&&m.push(`    MAXERRORS ${o.maxErrors}`),o.escapeChar!==null&&m.push(`    ESCAPECHAR '${o.escapeChar}'`),o.decimalDelim!==null&&m.push(`    DECIMALDELIM '${o.decimalDelim}'`),o.logDir!==null&&m.push(`    LOGDIR '${o.logDir}'`),o.quotedValue!==null&&m.push(`    QUOTEDVALUE '${o.quotedValue}'`),o.nullValue!==null&&m.push(`    NULLVALUE '${o.nullValue}'`),o.crInString!==null&&m.push(`    CRINSTRING ${o.crInString}`),o.truncString!==null&&m.push(`    TRUNCSTRING ${o.truncString}`),o.ctrlChars!==null&&m.push(`    CTRLCHARS ${o.ctrlChars}`),o.ignoreZero!==null&&m.push(`    IGNOREZERO ${o.ignoreZero}`),o.timeExtraZeros!==null&&m.push(`    TIMEEXTRAZEROS ${o.timeExtraZeros}`),o.y2Base!==null&&m.push(`    Y2BASE ${o.y2Base}`),o.fillRecord!==null&&m.push(`    FILLRECORD ${o.fillRecord}`),o.compress!==null&&m.push(`    COMPRESS ${o.compress}`),o.includeHeader!==null&&m.push(`    INCLUDEHEADER ${o.includeHeader}`),o.lfInString!==null&&m.push(`    LFINSTRING ${o.lfInString}`),o.dateStyle!==null&&m.push(`    DATESTYLE '${o.dateStyle}'`),o.dateDelim!==null&&m.push(`    DATEDELIM '${o.dateDelim}'`),o.timeDelim!==null&&m.push(`    TIMEDELIM '${o.timeDelim}'`),o.boolStyle!==null&&m.push(`    BOOLSTYLE '${o.boolStyle}'`),o.format!==null&&m.push(`    FORMAT '${o.format}'`),o.socketBufSize!==null&&m.push(`    SOCKETBUFSIZE ${o.socketBufSize}`),o.recordDelim!==null&&m.push(`    RECORDDELIM '${o.recordDelim}'`),o.maxRows!==null&&m.push(`    MAXROWS ${o.maxRows}`),o.requireQuotes!==null&&m.push(`    REQUIREQUOTES ${o.requireQuotes}`),o.recordLength!==null&&m.push(`    RECORDLENGTH ${o.recordLength}`),o.dateTimeDelim!==null&&m.push(`    DATETIMEDELIM '${o.dateTimeDelim}'`),o.rejectFile!==null&&m.push(`    REJECTFILE '${o.rejectFile}'`),m.push(");"),m.join(`
`)}async function Tn(u,e,t,s){let n=`
        SELECT 
            SCHEMA,
            OWNER,
            SYNONYM_NAME,
            REFOBJNAME,
            DESCRIPTION
        FROM ${e.toUpperCase()}.._V_SYNONYM
        WHERE DATABASE = '${e.toUpperCase()}'
            AND SCHEMA = '${t.toUpperCase()}'
            AND SYNONYM_NAME = '${s.toUpperCase()}'
    `,r=await u.query(n);if(r.length===0)throw new Error(`Synonym ${e}.${t}.${s} not found`);let i=r[0],o=q(e),h=q(i.OWNER||t),c=q(s),x=i.REFOBJNAME,p=[];if(p.push(`CREATE SYNONYM ${o}.${h}.${c} FOR ${x};`),i.DESCRIPTION){let m=i.DESCRIPTION.replace(/'/g,"''");p.push(`COMMENT ON SYNONYM ${c} IS '${m}';`)}return p.join(`
`)}async function Sn(u,e,t,s,n){let a=null;try{a=await Rt.connect(u);let r=n.toUpperCase();return r==="TABLE"?{success:!0,ddlCode:await yn(a,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:r==="VIEW"?{success:!0,ddlCode:await vn(a,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:r==="PROCEDURE"?{success:!0,ddlCode:await Cn(a,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:r==="EXTERNAL TABLE"?{success:!0,ddlCode:await bn(a,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:r==="SYNONYM"?{success:!0,ddlCode:await Tn(a,e,t,s),objectInfo:{database:e,schema:t,objectName:s,objectType:n}}:{success:!0,ddlCode:`-- DDL generation for ${n} not yet implemented
-- Object: ${e}.${t}.${s}
-- Type: ${n}
--
-- This feature can be extended to support:
-- - FUNCTION: Query _V_FUNCTION system table
-- - AGGREGATE: Query _V_AGGREGATE system table
`,objectInfo:{database:e,schema:t,objectName:s,objectType:n},note:`${n} DDL generation not yet implemented`}}catch(r){return{success:!1,error:`DDL generation error: ${r.message||r}`}}finally{if(a)try{await a.close()}catch{}}}var Rt,Mt=oe(()=>{"use strict";Rt=U(require("odbc"))});var Ot=ue((gs,_t)=>{"use strict";var{Buffer:Ee}=require("buffer"),tt=class{constructor(e=65536){this.chunkSize=e,this.chunks=[],this.currentBuffer=Ee.alloc(e),this.cursor=0}_ensureCapacity(e){this.cursor+e>this.chunkSize&&this._flush()}_flush(){this.cursor>0&&(this.chunks.push(this.currentBuffer.subarray(0,this.cursor)),this.currentBuffer=Ee.alloc(this.chunkSize),this.cursor=0)}write(e){let t=e.length,s=0;for(;t>0;){let n=this.chunkSize-this.cursor;n===0&&(this._flush(),n=this.chunkSize);let a=Math.min(t,n);e.copy(this.currentBuffer,this.cursor,s,s+a),this.cursor+=a,s+=a,t-=a}}writeByte(e){this._ensureCapacity(1),this.currentBuffer[this.cursor]=e,this.cursor++}writeInt32LE(e){this._ensureCapacity(4),this.currentBuffer.writeInt32LE(e,this.cursor),this.cursor+=4}writeDoubleLE(e){this._ensureCapacity(8),this.currentBuffer.writeDoubleLE(e,this.cursor),this.cursor+=8}writeString(e){let t=Ee.byteLength(e,"utf8");if(this.cursor+t<=this.chunkSize){this.cursor+=this.currentBuffer.write(e,this.cursor,"utf8");return}this._flush(),t>this.chunkSize?this.chunks.push(Ee.from(e,"utf8")):this.cursor+=this.currentBuffer.write(e,this.cursor,"utf8")}writeUtf16LE(e){let t=e.length*2;if(this.cursor+t<=this.chunkSize){for(let s=0;s<e.length;s++){let n=e.charCodeAt(s);this.currentBuffer[this.cursor++]=n&255,this.currentBuffer[this.cursor++]=n>>8&255}return}if(this._flush(),t>this.chunkSize)this.chunks.push(Ee.from(e,"utf16le"));else for(let s=0;s<e.length;s++){let n=e.charCodeAt(s);this.currentBuffer[this.cursor++]=n&255,this.currentBuffer[this.cursor++]=n>>8&255}}getChunks(){return this.cursor>0&&(this.chunks.push(this.currentBuffer.subarray(0,this.cursor)),this.currentBuffer=Ee.alloc(this.chunkSize),this.cursor=0),this.chunks}reset(){this.chunks=[],this.cursor=0}};_t.exports=tt});var Bt=ue((fs,Pt)=>{"use strict";var An=require("fs"),In=require("archiver"),{Readable:$t}=require("stream"),Lt=Ot(),Rn=/[\\\/*?\[\]:]/g,nt=class{constructor(e){this.filePath=e,this.output=An.createWriteStream(e),this.archive=In("zip"),this.archive.pipe(this.output),this.sheetCount=0,this.sheetList=[],this.sstDic=new Map,this.sstCntUnique=0,this.sstCntAll=0,this.colWidths=[],this._autofilterIsOn=!1,this._oaEpoch=Date.UTC(1899,11,30),this._sheet1Bytes=Buffer.from([129,1,0,147,1,23,203,4,2,0,64,0,0,0,0,0,0,255,255,255,255,255,255,255,255,0,0,0,0,148,1,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,133,1,0,137,1,30,220,3,0,0,0,0,0,0,0,0,0,0,0,0,64,0,0,0,100,0,0,0,0,0,0,0,0,0,0,0,152,1,36,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,138,1,0,134,1,0,37,6,1,0,2,14,0,128,149,8,2,5,0,38,0,229,3,12,255,255,255,255,8,0,44,1,0,0,0,0,145,1,0,37,6,1,0,2,14,0,128,128,8,2,5,0,38,0,0,25,0,0,0,0,0,0,0,0,44,1,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,7,12,0,0,0,0,0,0,0,0,0,0,0,0,146,1,0,151,4,66,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,221,3,2,16,0,220,3,48,102,102,102,102,102,102,230,63,102,102,102,102,102,102,230,63,0,0,0,0,0,0,232,63,0,0,0,0,0,0,232,63,51,51,51,51,51,51,211,63,51,51,51,51,51,51,211,63,37,6,1,0,0,16,0,128,128,24,16,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,38,0,130,1,0]),this._workbookBinStart=Buffer.from([131,1,0,128,1,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,120,0,108,0,1,0,0,0,55,0,1,0,0,0,54,0,5,0,0,0,50,0,52,0,51,0,50,0,54,0,153,1,12,32,0,1,0,0,0,0,0,0,0,0,0,37,6,1,0,3,15,0,128,151,16,52,24,0,0,0,67,0,58,0,92,0,115,0,113,0,108,0,115,0,92,0,84,0,101,0,115,0,116,0,121,0,90,0,97,0,112,0,105,0,115,0,117,0,88,0,108,0,115,0,98,0,92,0,38,0,37,6,1,0,0,16,0,128,129,24,130,1,0,0,0,0,0,0,0,0,47,0,0,0,49,0,51,0,95,0,110,0,99,0,114,0,58,0,49,0,95,0,123,0,49,0,54,0,53,0,48,0,56,0,68,0,54,0,57,0,45,0,67,0,70,0,56,0,55,0,45,0,52,0,55,0,54,0,57,0,45,0,56,0,52,0,53,0,54,0,45,0,68,0,52,0,65,0,52,0,48,0,49,0,49,0,51,0,49,0,53,0,54,0,55,0,125,0,47,0,0,0,47,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,38,0,135,1,0,37,6,1,0,2,16,0,128,128,24,16,0,0,0,0,13,0,0,0,255,255,255,255,0,0,0,0,38,0,158,1,29,0,0,0,0,158,22,0,0,180,105,0,0,232,38,0,0,88,2,0,0,0,0,0,0,0,0,0,0,120,136,1,0,143,1,0]),this._workbookBinMiddle=Buffer.from([144,1,0]),this._workbookBinEnd=Buffer.from([157,1,26,53,234,2,0,1,0,0,0,100,0,0,0,252,169,241,210,77,98,80,63,1,0,0,0,106,0,155,1,1,0,35,4,3,15,0,0,171,16,1,1,36,0,132,1,0]),this._stylesBin=Buffer.from([150,2,0,231,4,4,2,0,0,0,44,44,164,0,19,0,0,0,121,0,121,0,121,0,121,0,92,0,45,0,109,0,109,0,92,0,45,0,100,0,100,0,92,0,32,0,104,0,104,0,58,0,109,0,109,0,44,30,166,0,12,0,0,0,121,0,121,0,121,0,121,0,92,0,45,0,109,0,109,0,92,0,45,0,100,0,100,0,232,4,0,227,4,4,1,0,0,0,43,39,220,0,0,0,144,1,0,0,0,2,0,0,7,1,0,0,0,0,0,255,2,7,0,0,0,67,0,97,0,108,0,105,0,98,0,114,0,105,0,43,39,220,0,1,0,188,2,0,0,0,2,238,0,7,1,0,0,0,0,0,255,2,7,0,0,0,67,0,97,0,108,0,105,0,98,0,114,0,105,0,37,6,1,0,2,14,0,128,129,8,0,38,0,228,4,0,219,4,4,2,0,0,0,45,68,0,0,0,0,3,64,0,0,0,0,0,255,3,65,0,0,255,255,255,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,45,68,17,0,0,0,3,64,0,0,0,0,0,255,3,65,0,0,255,255,255,255,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,220,4,0,229,4,4,1,0,0,0,46,51,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,230,4,0,242,4,4,1,0,0,0,47,16,255,255,0,0,0,0,0,0,0,0,0,0,16,16,0,0,243,4,0,233,4,4,4,0,0,0,47,16,0,0,0,0,0,0,0,0,0,0,0,0,16,16,0,0,47,16,0,0,164,0,0,0,0,0,0,0,0,0,16,16,1,0,47,16,0,0,166,0,0,0,0,0,0,0,0,0,16,16,1,0,47,16,0,0,0,1,1,0,0,0,0,0,0,0,16,16,0,0,234,4,0,235,4,4,1,0,0,0,37,6,1,0,2,17,0,128,128,24,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,38,0,48,28,0,0,0,0,1,0,0,0,8,0,0,0,78,0,111,0,114,0,109,0,97,0,108,0,110,0,121,0,236,4,0,249,3,4,0,0,0,0,250,3,0,252,3,80,0,0,0,0,17,0,0,0,84,0,97,0,98,0,108,0,101,0,83,0,116,0,121,0,108,0,101,0,77,0,101,0,100,0,105,0,117,0,109,0,50,0,17,0,0,0,80,0,105,0,118,0,111,0,116,0,83,0,116,0,121,0,108,0,101,0,76,0,105,0,103,0,104,0,116,0,49,0,54,0,253,3,0,35,4,2,14,0,0,235,8,0,246,8,42,0,0,0,0,17,0,0,0,83,0,108,0,105,0,99,0,101,0,114,0,83,0,116,0,121,0,108,0,101,0,76,0,105,0,103,0,104,0,116,0,49,0,247,8,0,236,8,0,36,0,35,4,3,15,0,0,176,16,0,178,16,50,0,0,0,0,21,0,0,0,84,0,105,0,109,0,101,0,83,0,108,0,105,0,99,0,101,0,114,0,83,0,116,0,121,0,108,0,101,0,76,0,105,0,103,0,104,0,116,0,49,0,179,16,0,177,16,0,36,0,151,2,0]),this._binaryIndexBin=Buffer.from([42,24,0,0,0,0,32,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,149,2,0]),this._rRkIntegerLowerLimit=-1<<29,this._rRkIntegerUpperLimit=(1<<29)-1,this._autoFilterStartBytes=Buffer.from([161,1,16]),this._autoFilterEndBytes=Buffer.from([162,1,0]),this._stickHeaderA1bytes=Buffer.from([151,1,29,0,0,0,0,0,0,0,0,0,0,0,0,0,0,240,63,1,0,0,0,0,0,0,0,2,0,0,0,3]),this._magicFilterExcel2016Fix0=Buffer.from([225,2,0,229,2,0,234,2]),this._magicFilterExcel2016Fix1=Buffer.from([39,70,33,0,0,0,0,255,0,0,0,15,0,0,0,95,0,70,0,105,0,108,0,116,0,101,0,114,0,68,0,97,0,116,0,97,0,98,0,97,0,115,0,101,0,15,0,0,0,59,255,0]),this._magicFilterExcel2016Fix2=Buffer.from([0,0,0,0,255,255,255,255])}_sanitizeSheetName(e){if(!e||typeof e!="string")return`Sheet${this.sheetCount+1}`;let t=e.replace(Rn,"_");return t.length>31&&(t=t.substring(0,31)),t.trim().length===0&&(t=`Sheet${this.sheetCount+1}`),t}addSheet(e,t=!1){let s=this._sanitizeSheetName(e);this.sheetCount++,this.sheetList.push({name:s,pathInArchive:`xl/worksheets/sheet${this.sheetCount}.bin`,hidden:t,nameInArchive:`sheet${this.sheetCount}.bin`,sheetId:this.sheetCount,filterHeaderRange:null})}writeSheet(e,t=null,s=!0){let n=new Lt,a=0;if(e.length>0?a=e[0].length:t&&(a=t.length),this.colWidths=new Array(a).fill(-1),t)for(let c=0;c<a;c++){let p=1.25*(t[c]?t[c].length:0)+2;p>80&&(p=80),this.colWidths[c]<p&&(this.colWidths[c]=p)}for(let c=0;c<Math.min(e.length,100);c++){let x=e[c];for(let p=0;p<x.length;p++){let m=x[p],y=1.25*(m?m.toString().length:0)+2;y>80&&(y=80),this.colWidths[p]<y&&(this.colWidths[p]=y)}}let r=Buffer.from(this._sheet1Bytes),i=0,o=a;r.writeInt32LE(i,40),r.writeInt32LE(o,44),this.sheetCount!==1&&(r[54]=156),n.write(r.subarray(0,84)),s&&t&&n.write(this._stickHeaderA1bytes),n.write(r.subarray(84,159)),n.writeByte(134),n.writeByte(3);for(let c=i;c<o;c++){n.writeByte(0),n.writeByte(60),n.writeByte(18),n.writeInt32LE(c),n.writeInt32LE(c);let x=this.colWidths[c]>0?Math.floor(this.colWidths[c]):10;n.writeByte(0),n.writeByte(Math.max(0,Math.min(255,x))),n.writeByte(0),n.writeByte(0),n.writeByte(0),n.writeByte(0),n.writeByte(0),n.writeByte(0),n.writeByte(2)}n.writeByte(0),n.writeByte(135),n.writeByte(3),n.writeByte(0),n.write(r.subarray(159,175)),n.write(Buffer.from([38,0]));let h=0;if(t){this.createRowHeader(n,h,i,o);for(let c=0;c<t.length;c++)this.writeString(n,t[c],c,!0);h++}for(let c=0;c<e.length;c++){this.createRowHeader(n,h,i,o);let x=e[c];for(let p=0;p<x.length;p++){let m=x[p];m!=null&&(typeof m=="number"?Number.isInteger(m)?m>=this._rRkIntegerLowerLimit&&m<=this._rRkIntegerUpperLimit?this.writeRkNumberInteger(n,m,p):this.writeDouble(n,m,p):this.writeDouble(n,m,p):typeof m=="bigint"?this.writeString(n,m.toString(),p):typeof m=="boolean"?this.writeBool(n,m,p):m instanceof Date?this.writeDateTime(n,m,p):this.writeString(n,m.toString(),p))}h++}if(n.write(r.subarray(218,290)),s&&t){this._autofilterIsOn=!0;let c=0,x=e.length+1;n.write(this._autoFilterStartBytes);let p=Buffer.alloc(8);p.writeInt32LE(0,0),p.writeInt32LE(x-1,4),n.write(p);let m=Buffer.alloc(8);m.writeInt32LE(i,0),m.writeInt32LE(o-1,4),n.write(m),n.write(this._autoFilterEndBytes);let g=this.sheetList[this.sheetCount-1];g.filterData={startRow:0,endRow:e.length,startColumn:i,endColumn:o-1}}n.write(r.subarray(290)),this.archive.append($t.from(n.getChunks()),{name:this.sheetList[this.sheetCount-1].pathInArchive})}_getColumnLetter(e){if(e<26)return String.fromCharCode(65+e);if(e<702){let t=Math.floor(e/26)-1,s=e%26;return String.fromCharCode(65+t)+String.fromCharCode(65+s)}return"A"}createRowHeader(e,t,s,n){e.writeByte(0),e.writeByte(25),e.writeInt32LE(t),e.writeInt32LE(0),e.writeByte(44),e.writeByte(1),e.writeByte(0),e.writeByte(0),e.writeByte(0),e.writeByte(1),e.writeByte(0),e.writeByte(0),e.writeByte(0),e.writeInt32LE(s),e.writeInt32LE(n)}writeRkNumberInteger(e,t,s,n=0){e.writeByte(2),e.writeByte(12),e.writeInt32LE(s),e.writeByte(n),e.writeByte(0),e.writeByte(0),e.writeByte(0);let a=t<<2|2;e.writeInt32LE(a)}writeDouble(e,t,s,n=0){e.writeByte(5),e.writeByte(16),e.writeInt32LE(s),e.writeByte(n),e.writeByte(0),e.writeByte(0),e.writeByte(0),e.writeDoubleLE(t)}writeBool(e,t,s){e.writeByte(4),e.writeByte(9),e.writeInt32LE(s),e.writeInt32LE(0),e.writeByte(t?1:0)}writeDateTime(e,t,s){let n=t.getTimezoneOffset()*6e4,a=(t.getTime()-n-this._oaEpoch)/864e5;this.writeDouble(e,a,s,1)}writeString(e,t,s,n=!1){let a;this.sstDic.has(t)?a=this.sstDic.get(t):(a=this.sstCntUnique,this.sstDic.set(t,a),this.sstCntUnique++),this.sstCntAll++,e.writeByte(7),e.writeByte(12),e.writeInt32LE(s),e.writeByte(n?3:0),e.writeByte(0),e.writeByte(0),e.writeByte(0),e.writeInt32LE(a)}saveSst(){let e=new Lt;e.writeByte(159),e.writeByte(1),e.writeByte(8),e.writeInt32LE(this.sstCntUnique),e.writeInt32LE(this.sstCntAll);for(let[t,s]of this.sstDic){let n=t.length;e.writeByte(19);let a=5+2*n;if(a>=128){e.writeByte(128+a%128);let r=a>>7;r>=256?e.writeByte(128+r%128):e.writeByte(r),e.writeByte(a>>14),a>>14>0&&e.writeByte(0)}else e.writeByte(a&255),e.writeByte(a>>8&255);e.writeInt32LE(n),e.writeUtf16LE(t)}e.writeByte(160),e.writeByte(1),e.writeByte(0),this.archive.append($t.from(e.getChunks()),{name:"xl/sharedStrings.bin"})}_writeFilterDefinedName(e,t,s){let n=t.filterData,a=t.sheetId-1,r=Buffer.alloc(this._magicFilterExcel2016Fix1.length);this._magicFilterExcel2016Fix1.copy(r);let i=this._magicFilterExcel2016Fix1.length-2;r[7]=a,r[i]=s,e.push(r);let o=Buffer.alloc(8);o.writeInt32LE(n.startRow,0),o.writeInt32LE(n.endRow,4),e.push(o);let h=Buffer.alloc(4);h.writeInt16LE(n.startColumn,0),h.writeInt16LE(n.endColumn,2),e.push(h),e.push(this._magicFilterExcel2016Fix2)}finalize(){return new Promise((e,t)=>{try{this.saveSst(),this.archive.append(this._stylesBin,{name:"xl/styles.bin"});let s=[];s.push(this._workbookBinStart);for(let i of this.sheetList){let o=`rId${i.sheetId}`,h=16+i.name.length*2+o.length*2,c=Buffer.alloc(3+h);c[0]=156,c[1]=1,c[2]=h;let x=3;c.writeInt32LE(i.hidden?1:0,x),x+=4,c.writeInt32LE(i.sheetId,x),x+=4,c.writeInt32LE(o.length,x),x+=4;for(let p=0;p<o.length;p++){let m=o.charCodeAt(p);c[x++]=m&255,c[x++]=m>>8&255}c.writeInt32LE(i.name.length,x),x+=4;for(let p=0;p<i.name.length;p++){let m=i.name.charCodeAt(p);c[x++]=m&255,c[x++]=m>>8&255}s.push(c)}if(s.push(this._workbookBinMiddle),this._autofilterIsOn){let i=this.sheetList.filter(h=>h.filterData),o=i.length;if(o>0){s.push(this._magicFilterExcel2016Fix0);let h=16+(o-1)*12,c=Buffer.from([h,o,0,0,0]);s.push(c);for(let x=0;x<o;x++){let p=i[x].sheetId-1,m=Buffer.alloc(12);m.writeInt32LE(0,0),m[4]=p,m[8]=p,s.push(m)}s.push(Buffer.from([226,2,0]));for(let x=0;x<o;x++){let p=i[x];this._writeFilterDefinedName(s,p,x)}}}s.push(this._workbookBinEnd),this.archive.append(Buffer.concat(s),{name:"xl/workbook.bin"});for(let i of this.sheetList)this.archive.append(this._binaryIndexBin,{name:`xl/worksheets/binaryIndex${i.sheetId}.bin`});let n=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="bin" ContentType="application/vnd.ms-excel.sheet.binary.macroEnabled.main"/>
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>`;for(let i of this.sheetList)n+=`<Override PartName="/${i.pathInArchive}" ContentType="application/vnd.ms-excel.worksheet"/>`,n+=`<Override PartName="/xl/worksheets/binaryIndex${i.sheetId}.bin" ContentType="application/vnd.ms-excel.binIndexWs"/>`;n+=`<Override PartName="/xl/styles.bin" ContentType="application/vnd.ms-excel.styles"/>
<Override PartName="/xl/sharedStrings.bin" ContentType="application/vnd.ms-excel.sharedStrings"/>
</Types>`,this.archive.append(n,{name:"[Content_Types].xml"});let a=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`;for(let i of this.sheetList){let o=`rId${i.sheetId}`;a+=`<Relationship Id="${o}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/${i.nameInArchive}"/>`}a+=`<Relationship Id="rId${this.sheetList.length+2}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.bin"/>
<Relationship Id="rId${this.sheetList.length+3}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.bin"/>
</Relationships>`,this.archive.append(a,{name:"xl/_rels/workbook.bin.rels"}),this.archive.append(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.bin"/>
</Relationships>`,{name:"_rels/.rels"});for(let i of this.sheetList){let o=`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.microsoft.com/office/2006/relationships/xlBinaryIndex" Target="binaryIndex${i.sheetId}.bin"/>
</Relationships>`;this.archive.append(o,{name:`xl/worksheets/_rels/${i.nameInArchive}.rels`})}this.output.on("close",()=>e()),this.archive.on("error",i=>t(i)),this.archive.finalize()}catch(s){t(s)}})}};Pt.exports=nt});var me={};ie(me,{copyFileToClipboard:()=>ot,exportCsvToXlsb:()=>Mn,exportQueryToXlsb:()=>Nn,getTempFilePath:()=>_n});async function Nn(u,e,t,s=!1,n){let a=null;try{n&&n("Connecting to database..."),a=await Dn.connect(u),n&&n("Executing query...");let r=await a.query(e);if(!r||!r.columns||r.columns.length===0)return{success:!1,message:"Query did not return any results (no columns)"};let i=r.columns.length;n&&n(`Query returned ${i} columns`);let o=r.columns.map(w=>w.name),h=r.map(w=>o.map(A=>w[A])),c=h.length;n&&n(`Writing ${c.toLocaleString()} rows to XLSB: ${t}`);let x=new zt(t);x.addSheet("Query Results"),x.writeSheet(h,o),x.addSheet("SQL Code");let p=[["SQL Query:"],...e.split(`
`).map(w=>[w])];x.writeSheet(p,null,!1),await x.finalize();let g=st.statSync(t).size/(1024*1024);n&&(n("XLSB file created successfully (via XlsbWriter)"),n(`  - Rows: ${c.toLocaleString()}`),n(`  - Columns: ${i}`),n(`  - File size: ${g.toFixed(1)} MB`),n(`  - Location: ${t}`));let y={success:!0,message:`Successfully exported ${c} rows to ${t}`,details:{rows_exported:c,columns:i,file_size_mb:parseFloat(g.toFixed(1)),file_path:t}};if(s){n&&n("Copying to clipboard...");let w=await ot(t);y.details&&(y.details.clipboard_success=w)}return y}catch(r){return{success:!1,message:`Export error: ${r.message||r}`}}finally{if(a)try{await a.close()}catch{}}}async function Mn(u,e,t=!1,s,n){try{n&&n("Reading CSV content...");let a=w=>{let A=[],M=0,D=!1;for(let z=0;z<w.length;z++)if(w[z]==='"')D=!D;else if(w[z]===","&&!D){let _=w.substring(M,z);_.startsWith('"')&&_.endsWith('"')&&(_=_.slice(1,-1).replace(/""/g,'"')),A.push(_),M=z+1}let L=w.substring(M);return L.startsWith('"')&&L.endsWith('"')&&(L=L.slice(1,-1).replace(/""/g,'"')),A.push(L),A},r=u.split(/\r?\n/),i=[];for(let w of r)w.trim()&&i.push(a(w));if(i.length===0)return{success:!1,message:"CSV content is empty"};let o=i[0],h=i.slice(1),c=h.length,x=o.length;n&&n(`Writing ${c.toLocaleString()} rows to XLSB: ${e}`);let p=new zt(e);if(p.addSheet("CSV Data"),p.writeSheet(h,o),s?.sql){p.addSheet("SQL");let w=[["SQL Query:"],...s.sql.split(`
`).map(A=>[A])];p.writeSheet(w,null,!1)}else{let w=s?.source||"Clipboard";p.addSheet("CSV Source");let A=[["CSV Source:"],[w]];p.writeSheet(A,null,!1)}await p.finalize();let g=st.statSync(e).size/(1024*1024);n&&(n("XLSB file created successfully (via XlsbWriter)"),n(`  - Rows: ${c.toLocaleString()}`),n(`  - Columns: ${x}`),n(`  - File size: ${g.toFixed(1)} MB`),n(`  - Location: ${e}`));let y={success:!0,message:`Successfully exported ${c} rows from CSV to ${e}`,details:{rows_exported:c,columns:x,file_size_mb:parseFloat(g.toFixed(1)),file_path:e}};if(t){n&&n("Copying to clipboard...");let w=await ot(e);y.details&&(y.details.clipboard_success=w)}return y}catch(a){return{success:!1,message:`Export error: ${a.message||a}`}}}async function ot(u){return He.platform()!=="win32"?(console.error("Clipboard file copy is only supported on Windows"),!1):new Promise(e=>{try{let t=ye.normalize(ye.resolve(u)),s=`Set-Clipboard -Path "${t.replace(/"/g,'`"')}"`,n=(0,Ft.spawn)("powershell.exe",["-NoProfile","-NonInteractive","-Command",s]),a="";n.stderr.on("data",r=>{a+=r.toString()}),n.on("close",r=>{r!==0?(console.error(`PowerShell clipboard copy failed: ${a}`),e(!1)):(console.log(`File copied to clipboard: ${t}`),e(!0))}),n.on("error",r=>{console.error(`Error spawning PowerShell: ${r.message}`),e(!1)})}catch(t){console.error(`Error copying file to clipboard: ${t.message}`),e(!1)}})}function _n(){let u=He.tmpdir(),t=`netezza_export_${Date.now()}.xlsb`;return ye.join(u,t)}var st,ye,He,Ft,zt,Dn,ge=oe(()=>{"use strict";st=U(require("fs")),ye=U(require("path")),He=U(require("os")),Ft=require("child_process"),zt=Bt(),Dn=require("odbc")});var Ut={};ie(Ut,{exportToCsv:()=>On});async function On(u,e,t,s,n){if(!rt)throw new Error("odbc package not installed. Please run: npm install odbc");let a=await rt.connect(e);try{n&&n.report({message:"Executing query..."});let r=await a.query(t,{cursor:!0,fetchSize:1e3});n&&n.report({message:"Writing to CSV..."});let i=kt.createWriteStream(s,{encoding:"utf8",highWaterMark:64*1024}),o=[];r.columns&&(o=r.columns.map(m=>m.name)),o.length>0&&i.write(o.map(it).join(",")+`
`);let h=0,c=[],x=[],p=100;do{c=await r.fetch();for(let m of c){h++;let g;if(o.length>0?g=o.map(y=>it(m[y])):g=Object.values(m).map(y=>it(y)),x.push(g.join(",")),x.length>=p){let y=i.write(x.join(`
`)+`
`);x=[],y||await new Promise(w=>i.once("drain",w))}}n&&c.length>0&&n.report({message:`Processed ${h} rows...`})}while(c.length>0&&!r.noData);x.length>0&&i.write(x.join(`
`)+`
`),await r.close(),i.end(),await new Promise((m,g)=>{i.on("finish",m),i.on("error",g)}),n&&n.report({message:`Completed: ${h} rows exported`})}finally{try{await a.close()}catch(r){console.error("Error closing connection:",r)}}}function it(u){if(u==null)return"";let e="";return typeof u=="bigint"?u>=Number.MIN_SAFE_INTEGER&&u<=Number.MAX_SAFE_INTEGER?e=Number(u).toString():e=u.toString():u instanceof Date?e=u.toISOString():u instanceof Buffer?e=u.toString("hex"):typeof u=="object"?e=JSON.stringify(u):e=String(u),e.includes('"')||e.includes(",")||e.includes(`
`)||e.includes("\r")?`"${e.replace(/"/g,'""')}"`:e}var kt,rt,Ht=oe(()=>{"use strict";kt=U(require("fs"));try{rt=require("odbc")}catch{console.error("odbc package not installed. Run: npm install odbc")}});var ct=ue((ws,Wt)=>{"use strict";var at=class{constructor(){this.fieldCount=0,this.rowCount=0,this.actualSheetName="",this.resultsCount=0,this._oaEpoch=Date.UTC(1899,11,30)}async open(e,t=!0,s=!1){throw new Error("Method 'open' must be implemented.")}async close(){}read(){throw new Error("Method 'read' must be implemented.")}getSheetNames(){throw new Error("Method 'getSheetNames' must be implemented.")}getValue(e){throw new Error("Method 'getValue' must be implemented.")}dispose(){}getDateTimeFromOaDate(e){let t=e*864e5+this._oaEpoch;return new Date(t)}};Wt.exports=at});var Vt=ue((Es,qt)=>{"use strict";var $n=require("yauzl"),Ln=ct(),lt=class extends Ln{constructor(){super(),this.zipfile=null,this.sharedStrings=[],this.styles=[],this.sheetNames=[],this.sheets=[],this._currentSheetIndex=-1,this._sheetXml=null,this._xmlPos=0,this._currentRow=[],this.fieldCount=0}async open(e,t=!0){return new Promise((s,n)=>{$n.open(e,{lazyEntries:!0,autoClose:!1},async(a,r)=>{if(a)return n(a);this.zipfile=r,this.entries=new Map,r.on("entry",i=>{this.entries.set(i.fileName,i),r.readEntry()}),r.on("end",async()=>{try{let i=await this._readZipEntryContent("xl/_rels/workbook.xml.rels"),o={};if(i){let x=/<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g,p;for(;(p=x.exec(i))!==null;)o[p[1]]=p[2];for(let m in o){let g=o[m];g.startsWith("/")&&(g=g.substring(1)),!g.startsWith("xl/")&&!g.startsWith("worksheets/")&&!g.startsWith("theme/")&&!g.startsWith("styles")&&g.startsWith("sharedStrings")}}let h=await this._readZipEntryContent("xl/workbook.xml");if(h){let x=/<sheet[^>]*name="([^"]*)"[^>]*sheetId="([^"]*)"[^>]*r:id="([^"]*)"/g,p;for(;(p=x.exec(h))!==null;){let m=this._unescapeXml(p[1]),g=p[2],y=p[3],A=o[y];A.startsWith("xl/")||(A="xl/"+A),this.sheetNames.push(m),this.sheets.push({name:m,sheetId:g,rId:y,path:A})}}if(t){let x=await this._readZipEntryContent("xl/sharedStrings.xml");x&&this._parseSharedStrings(x)}let c=await this._readZipEntryContent("xl/styles.xml");c&&this._parseStyles(c),this.resultsCount=this.sheets.length,this._currentSheetIndex=-1,s()}catch(i){n(i)}}),r.readEntry()})})}async close(){this.zipfile&&this.zipfile.close()}async _readZipEntryContent(e){let t=this.entries.get(e);return t?new Promise((s,n)=>{this.zipfile.openReadStream(t,(a,r)=>{if(a)return n(a);let i=[];r.on("data",o=>i.push(o)),r.on("end",()=>{s(Buffer.concat(i).toString("utf8"))}),r.on("error",n)})}):null}_unescapeXml(e){return e?e.indexOf("&")===-1?e:e.replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&quot;/g,'"').replace(/&apos;/g,"'"):""}_parseSharedStrings(e){let t=0;for(;;){let s=e.indexOf("<si>",t);if(s===-1)break;let n=e.indexOf("</si>",s);if(n===-1)break;let a=e.substring(s,n),r="",i=0;for(;;){let o=a.indexOf("<t",i);if(o===-1)break;let h=a.indexOf(">",o),c=a.indexOf("</t>",h);if(c===-1)break;r+=a.substring(h+1,c),i=c+4}this.sharedStrings.push(this._unescapeXml(r)),t=n+5}}_parseStyles(e){let t=/<numFmt\s+[^>]*numFmtId="(\d+)"[^>]*formatCode="([^"]*)"/g,s;for(this.customDateFormats=new Set;(s=t.exec(e))!==null;){let a=parseInt(s[1]),r=s[2];(r.toLowerCase().includes("yy")||r.toLowerCase().includes("mm")||r.toLowerCase().includes("dd")||r.toLowerCase().includes("h:mm"))&&this.customDateFormats.add(a)}this.xfIdToNumFmtId=[];let n=e.indexOf("<cellXfs");if(n!==-1){let a=e.indexOf("</cellXfs>",n);if(a!==-1){let r=e.substring(n,a),i=/<xf\s+[^>]*numFmtId="(\d+)"/g;for(;(s=i.exec(r))!==null;){let o=parseInt(s[1]);this.xfIdToNumFmtId.push(o)}}}}getSheetNames(){return this.sheetNames}async read(){return this._currentSheetIndex===-1&&(this._currentSheetIndex=0,await this._initSheet(this._currentSheetIndex)),!!this._readNextRow()}async _initSheet(e){if(e>=this.sheets.length)return;let t=this.sheets[e];this.actualSheetName=t.name,this._sheetXml=await this._readZipEntryContent(t.path),this._sheetXml||(this._sheetXml=""),this._xmlPos=0;let s=this._sheetXml.indexOf("<sheetData>");s!==-1&&(this._xmlPos=s+11)}_readNextRow(){if(!this._sheetXml)return!1;let e=this._sheetXml.indexOf("<row",this._xmlPos);if(e===-1)return!1;let t=this._sheetXml.indexOf("</row>",e);return t===-1?!1:(this._xmlPos=t+6,this._parseRowByIndex(e,t),!0)}_parseRowByIndex(e,t){this._currentRow=[];let s=this._sheetXml,n=s.indexOf(">",e)+1;for(;n<t;){let a=s.indexOf("<c",n);if(a===-1||a>=t)break;let r=s.indexOf(">",a),i=-1,o="n",h=0,c=a+2;for(;c<r;){for(;s.charCodeAt(c)<=32&&c<r;)c++;if(c>=r)break;let m=c;for(;s.charCodeAt(c)!==61&&c<r;)c++;let g=s.substring(m,c);if(c++,s.charCodeAt(c)===34){c++;let y=c;for(;s.charCodeAt(c)!==34&&c<r;)c++;let w=s.substring(y,c);if(c++,g==="r"){let A=0;for(;A<w.length&&w.charCodeAt(A)>=65;)A++;i=this._columnLetterToIndex(w.substring(0,A))}else g==="t"?o=w:g==="s"&&(h=parseInt(w,10))}}let x=null;if(s.charCodeAt(r-1)===47)n=r+1;else{let m=s.indexOf("</c>",r),g=m!==-1&&m<t?m:t,y=s.indexOf("<v>",r);if(y!==-1&&y<g){let w=y+3,A=s.indexOf("</v>",w);A!==-1&&A<g&&(x=s.substring(w,A))}else if(o==="inlineStr"){let w=s.indexOf("<t",r);if(w!==-1&&w<g){let A=s.indexOf(">",w)+1,M=s.indexOf("</t>",A);M!==-1&&M<g&&(x=s.substring(A,M))}}n=g+4}let p=null;if(x!==null){if(o==="s"){let m=parseInt(x,10);p=this.sharedStrings[m]}else if(o==="b")p=x==="1"||x==="true";else if(o==="inlineStr")p=this._unescapeXml(x);else if((o==="n"||o==="")&&(p=parseFloat(x),h>0&&this.xfIdToNumFmtId)){let m=0;if(h<this.xfIdToNumFmtId.length&&(m=this.xfIdToNumFmtId[h]),m>=14&&m<=22||m>=45&&m<=47||this.customDateFormats&&this.customDateFormats.has(m))try{p=this.getDateTimeFromOaDate(p)}catch{}}}i!==-1&&(this._currentRow[i]=p,i>=this.fieldCount&&(this.fieldCount=i+1))}}_columnLetterToIndex(e){let t=0,s=e.length;for(let n=0;n<s;n++)t+=(e.charCodeAt(n)-64)*Math.pow(26,s-n-1);return t-1}getValue(e){return e<0||e>=this._currentRow.length?null:this._currentRow[e]}};qt.exports=lt});var Xt=ue((vs,Jt)=>{"use strict";var{TextDecoder:ys}=require("util"),dt=class{constructor(e){if(!Buffer.isBuffer(e))throw new Error("BiffReaderWriter expects a Buffer");this.buffer=e,this.pos=0,this.length=e.length,this._isSheet=!1,this._workbookId=0,this._recId=null,this._workbookName=null,this._inCellXf=!1,this._inCellStyleXf=!1,this._inNumberFormat=!1,this._parentCellStyleXf=0,this._numberFormatIndex=0,this._format=0,this._formatString=null,this._sharedStringValue=null,this._sharedStringUniqueCount=0,this._cellType=0,this._intValue=0,this._doubleVal=0,this._boolValue=!1,this._stringValue=null,this._columnNum=-1,this._xfIndex=0,this._readCell=!1,this._rowIndex=-1,this._tempBuf=Buffer.alloc(128),this._xfIndexToNumFmtId=[],this._customNumFmts=new Set}_tryReadVariableValue(){if(this.pos>=this.length)return null;let e=this.buffer[this.pos++],t=(e&127)>>>0;if((e&128)===0)return t;if(this.pos>=this.length)return null;let s=this.buffer[this.pos++];if(t=((s&127)<<7|t)>>>0,(s&128)===0)return t;if(this.pos>=this.length)return null;let n=this.buffer[this.pos++];return t=((n&127)<<14|t)>>>0,(n&128)===0?t:this.pos>=this.length?null:(t=((this.buffer[this.pos++]&127)<<21|t)>>>0,t)}_getDWord(e,t){return e.readUInt32LE(t)}_getInt32(e,t){return e.readInt32LE(t)}_getWord(e,t){return e.readUInt16LE(t)}_getString(e,t,s){let n=t,a=t+s*2;return a>e.length?"":e.toString("utf16le",n,a)}_getNullableString(e,t){let s=this._getDWord(e,t.val);if(t.val+=4,s===4294967295)return null;let n=this._getString(e,t.val,s);return t.val+=s*2,n}readWorkbook(){let e=this._tryReadVariableValue(),t=this._tryReadVariableValue();if(e===null||t===null)return!1;let s=this.pos;if(s+t>this.length)return!1;let n=this.buffer.subarray(s,s+t);if(this.pos+=t,this._isSheet=!1,e===156){this._workbookId=this._getDWord(n,4);let a={val:8};this._recId=this._getNullableString(n,a);let r=this._getDWord(n,a.val);this._workbookName=this._getString(n,a.val+4,r),this._isSheet=!0}return!0}readSharedStrings(){let e=this._tryReadVariableValue(),t=this._tryReadVariableValue();if(e===null||t===null)return!1;let s=this.pos;if(s+t>this.length)return!1;let n=this.buffer.subarray(s,s+t);if(this.pos+=t,this._sharedStringValue=null,e===19){let a=this._getDWord(n,1);this._sharedStringValue=this._getString(n,5,a)}else e===159&&(this._sharedStringUniqueCount=this._getDWord(n,4));return!0}readStyles(){let e=this._tryReadVariableValue(),t=this._tryReadVariableValue();if(e===null||t===null)return!1;let s=this.pos;if(s+t>this.length)return!1;let n=this.buffer.subarray(s,s+t);this.pos+=t;let a=617,r=618,i=626,o=627,h=615,c=616,x=47,p=44;switch(e){case a:this._inCellXf=!0;break;case r:this._inCellXf=!1;break;case i:this._inCellStyleXf=!0;break;case o:this._inCellStyleXf=!1;break;case h:this._inNumberFormat=!0;break;case c:this._inNumberFormat=!1;break;case x:this._inCellXf&&(this._parentCellStyleXf=this._getWord(n,0),this._numberFormatIndex=this._getWord(n,2),this._xfIndexToNumFmtId.push(this._numberFormatIndex));break;case p:if(this._inNumberFormat){this._format=this._getWord(n,0);let m=this._getDWord(n,2);this._formatString=this._getString(n,6,m);let g=this._formatString.toLowerCase();(g.includes("yy")||g.includes("mm")||g.includes("dd")||g.includes("h:mm"))&&this._customNumFmts.add(this._format)}break}return!0}_getRkNumber(e,t){let s=e[t],n=0;if((s&2)!==0)n=e.readInt32LE(t)>>2;else{let r=e.readInt32LE(t)&4294967292,i=Buffer.alloc(8);i.writeInt32LE(0,0),i.writeInt32LE(r,4),n=i.readDoubleLE(0)}return(s&1)!==0&&(n/=100),n}readWorksheet(){let e=this._tryReadVariableValue(),t=this._tryReadVariableValue();if(e===null||t===null)return!1;let s=this.pos;if(s+t>this.length)return!1;let n=this.buffer.subarray(s,s+t);this.pos+=t,this._readCell=!1,this._columnNum=-1;let a=0,r=1,i=2,o=3,h=4,c=5,x=6,p=7,m=8,g=9,y=10,w=11;switch(e){case a:this._rowIndex=this._getInt32(n,0);break;case r:case o:case w:this._readCell=!0,this._cellType=0;break;case i:this._doubleVal=this._getRkNumber(n,8),this._readCell=!0,this._cellType=3;break;case h:case y:this._boolValue=n[8]===1,this._readCell=!0,this._cellType=4;break;case g:case c:this._doubleVal=n.readDoubleLE(8),this._readCell=!0,this._cellType=3;break;case x:case m:{let A=this._getDWord(n,8);this._stringValue=this._getString(n,12,A),this._readCell=!0,this._cellType=5;break}case p:this._intValue=this._getDWord(n,8),this._readCell=!0,this._cellType=2;break}return this._readCell&&(this._columnNum=this._getDWord(n,0),this._xfIndex=this._getDWord(n,4)&16777215),!0}};Jt.exports=dt});var jt=ue((Cs,Yt)=>{"use strict";var Pn=require("adm-zip"),Bn=ct(),We=Xt(),ut=class extends Bn{constructor(){super(),this.zip=null,this.sharedStrings=[],this.styles=[],this.sheetNames=[],this.sheets=[],this._currentSheetIndex=-1,this._reader=null,this._currentRow=[],this._pendingRowIndex=-1,this._eof=!1}open(e,t=!0){this.zip=new Pn(e);let s=this.zip.getEntry("xl/workbook.bin");if(s){let r=s.getData(),i=new We(r);for(;i.readWorkbook();)if(i._isSheet){let o=i._workbookName,h=i._recId;this.sheetNames.push(o),this.sheets.push({name:o,rId:h,path:null})}}let n=this.zip.getEntry("xl/_rels/workbook.bin.rels");if(n){let r=n.getData().toString("utf8"),i=/<Relationship[^>]*Id="([^"]*)"[^>]*Target="([^"]*)"/g,o,h={};for(;(o=i.exec(r))!==null;)h[o[1]]=o[2];for(let c of this.sheets){let x=h[c.rId];x&&(x.startsWith("/")&&(x=x.substring(1)),x.startsWith("xl/")||(x="xl/"+x),c.path=x)}}if(t){let r="xl/sharedStrings.bin";this.zip.getEntry(r)&&this._readSharedStrings(r)}let a="xl/styles.bin";this.zip.getEntry(a)&&this._readStyles(a),this.resultsCount=this.sheets.length,this._currentSheetIndex=-1}_readSharedStrings(e){let t=this.zip.getEntry(e);if(!t)return;let s=new We(t.getData());for(;s.readSharedStrings();)s._sharedStringValue!==null&&this.sharedStrings.push(s._sharedStringValue)}_readStyles(e){let t=this.zip.getEntry(e);if(!t)return;let s=new We(t.getData());for(;s.readStyles(););this.xfIdToNumFmtId=s._xfIndexToNumFmtId,this.customDateFormats=s._customNumFmts}getSheetNames(){return this.sheetNames}read(){if(this._currentSheetIndex===-1&&(this._currentSheetIndex=0,!this._initSheet(0))||this._eof)return!1;for(this._currentRow=[];;){if(!this._reader.readWorksheet())return this._eof=!0,!0;if(this._reader._rowIndex!==-1&&this._reader._rowIndex!==this._pendingRowIndex){let t=this._pendingRowIndex;return this._pendingRowIndex=this._reader._rowIndex,!0}if(this._reader._readCell){let t=this._reader._columnNum,s=null;switch(this._reader._cellType){case 2:s=this.sharedStrings[this._reader._intValue];break;case 3:s=this._reader._doubleVal;let n=this._reader._xfIndex,a=0;if(this.xfIdToNumFmtId&&n<this.xfIdToNumFmtId.length&&(a=this.xfIdToNumFmtId[n]),a>=14&&a<=22||a>=45&&a<=47||this.customDateFormats&&this.customDateFormats.has(a))try{s=this.getDateTimeFromOaDate(s)}catch{}break;case 4:s=this._reader._boolValue;break;case 5:s=this._reader._stringValue;break;default:s=null}this._currentRow[t]=s,t>=this.fieldCount&&(this.fieldCount=t+1)}}}_initSheet(e){if(e>=this.sheets.length)return!1;let t=this.sheets[e],s=this.zip.getEntry(t.path);if(!s)return!1;for(this._reader=new We(s.getData()),this._eof=!1,this._pendingRowIndex=-1;this._reader.readWorksheet();)if(this._reader._rowIndex!==-1)return this._pendingRowIndex=this._reader._rowIndex,!0;return this._eof=!0,!1}getValue(e){return e<0||e>=this._currentRow.length?null:this._currentRow[e]}};Yt.exports=ut});var Kt=ue((bs,Qt)=>{"use strict";var Fn=Vt(),zn=jt(),kn=require("path"),ht=class{static create(e){let t=kn.extname(e).toLowerCase();if(t===".xlsx")return new Fn;if(t===".xlsb")return new zn;throw new Error(`Unsupported extension: ${t}`)}};Qt.exports=ht});var Gt={};ie(Gt,{ColumnTypeChooser:()=>ve,NetezzaDataType:()=>ae,NetezzaImporter:()=>qe,importDataToNetezza:()=>Un});async function Un(u,e,t,s,n){let a=Date.now();try{if(!u||!Q.existsSync(u))return{success:!1,message:`Source file not found: ${u}`};if(!e)return{success:!1,message:"Target table name is required"};if(!t)return{success:!1,message:"Connection string is required"};let i=Q.statSync(u).size,o=le.extname(u).toLowerCase(),h=[".csv",".txt",".xlsx",".xlsb"];if(!h.includes(o))return{success:!1,message:`Unsupported file format: ${o}. Supported: ${h.join(", ")}`};n?.("Starting import process..."),n?.(`  Source file: ${u}`),n?.(`  Target table: ${e}`),n?.(`  File size: ${i.toLocaleString()} bytes`),n?.(`  File format: ${o}`);let c=new qe(u,e,t);await c.analyzeDataTypes(n),n?.("Using file-based import...");let x=await c.createDataFile(n),p=c.generateCreateTableSql();if(n?.("Generated SQL:"),n?.(p),n?.("Connecting to Netezza..."),!xt)throw new Error("ODBC module not available");let m=await xt.connect(t);try{n?.("Executing CREATE TABLE with EXTERNAL data..."),await m.query(p),n?.("Import completed successfully")}finally{await m.close();try{Q.existsSync(x)&&(Q.unlinkSync(x),n?.("Temporary data file cleaned up"))}catch(y){n?.(`Warning: Could not clean up temp file: ${y.message}`)}}let g=(Date.now()-a)/1e3;return{success:!0,message:"Import completed successfully",details:{sourceFile:u,targetTable:e,fileSize:i,format:o,rowsProcessed:c.getRowsCount(),rowsInserted:c.getRowsCount(),processingTime:`${g.toFixed(1)}s`,columns:c.getSqlHeaders().length,detectedDelimiter:c.getCsvDelimiter()}}}catch(r){let i=(Date.now()-a)/1e3;return{success:!1,message:`Import failed: ${r.message}`,details:{processingTime:`${i.toFixed(1)}s`}}}}var Q,le,pt,xt,ae,ve,qe,mt=oe(()=>{"use strict";Q=U(require("fs")),le=U(require("path"));try{pt=Kt()}catch(u){console.error("ExcelHelpers/ReaderFactory module not available",u)}try{xt=require("odbc")}catch{console.error("ODBC module not available")}ae=class{constructor(e,t,s,n){this.dbType=e;this.precision=t;this.scale=s;this.length=n}toString(){return["BIGINT","DATE","DATETIME"].includes(this.dbType)?this.dbType:this.dbType==="NUMERIC"?`${this.dbType}(${this.precision},${this.scale})`:this.dbType==="NVARCHAR"?`${this.dbType}(${this.length})`:`TODO !!! ${this.dbType}`}},ve=class{constructor(){this.decimalDelimInCsv=".";this.firstTime=!0;this.currentType=new ae("BIGINT")}getType(e){let t=this.currentType.dbType,s=e.length;if(t==="BIGINT"&&/^\d+$/.test(e)&&s<15)return this.firstTime=!1,new ae("BIGINT");let n=(e.match(new RegExp(`\\${this.decimalDelimInCsv}`,"g"))||[]).length;if(["BIGINT","NUMERIC"].includes(t)&&n<=1){let r=e.replace(this.decimalDelimInCsv,"");if(/^\d+$/.test(r)&&s<15&&(!r.startsWith("0")||n>0))return this.firstTime=!1,new ae("NUMERIC",16,6)}if((t==="DATE"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=8&&s<=10){let r=e.split("-");if(r.length===3&&r.every(i=>/^\d+$/.test(i)))try{let i=new Date(parseInt(r[0]),parseInt(r[1])-1,parseInt(r[2]));if(!isNaN(i.getTime()))return this.firstTime=!1,new ae("DATE")}catch{}}if((t==="DATETIME"||this.firstTime)&&(e.match(/-/g)||[]).length===2&&s>=12&&s<=20){let r=e.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[\s|T](\d{2}):(\d{2})(:?(\d{2}))?$/);if(r)try{let i=r[7]?parseInt(r[7]):0,o=new Date(parseInt(r[1]),parseInt(r[2])-1,parseInt(r[3]),parseInt(r[4]),parseInt(r[5]),i);if(!isNaN(o.getTime()))return this.firstTime=!1,new ae("DATETIME")}catch{}}let a=Math.max(s+5,20);return this.currentType.length!==void 0&&a<this.currentType.length&&(a=this.currentType.length),this.firstTime=!1,new ae("NVARCHAR",void 0,void 0,a)}refreshCurrentType(e){return this.currentType=this.getType(e),this.currentType}},qe=class{constructor(e,t,s,n){this.delimiter="	";this.delimiterPlain="\\t";this.recordDelim=`
`;this.recordDelimPlain="\\n";this.escapechar="\\";this.csvDelimiter=",";this.excelData=[];this.isExcelFile=!1;this.sqlHeaders=[];this.dataTypes=[];this.rowsCount=0;this.valuesToEscape=[];this.filePath=e,this.targetTable=t,this.connectionString=s,this.logDir=n||le.join(le.dirname(e),"netezza_logs");let a=le.extname(e).toLowerCase();this.isExcelFile=[".xlsx",".xlsb"].includes(a);let r=Math.floor(Math.random()*1e3);this.pipeName=`\\\\.\\pipe\\NETEZZA_IMPORT_${r}`,this.valuesToEscape=[this.escapechar,this.recordDelim,"\r",this.delimiter],Q.existsSync(this.logDir)||Q.mkdirSync(this.logDir,{recursive:!0})}detectCsvDelimiter(){let t=Q.readFileSync(this.filePath,"utf-8").split(`
`)[0]||"";t.startsWith("\uFEFF")&&(t=t.slice(1));let s=[";","	","|",","],n={};for(let r of s)n[r]=(t.match(new RegExp(r==="|"?"\\|":r,"g"))||[]).length;let a=Math.max(...Object.values(n));a>0&&(this.csvDelimiter=Object.keys(n).find(r=>n[r]===a)||",")}cleanColumnName(e){let t=String(e).trim();return t=t.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!t||/^\d/.test(t))&&(t="COL_"+t),t}parseCsvLine(e){let t=[],s="",n=!1;for(let a=0;a<e.length;a++){let r=e[a];r==='"'?n&&e[a+1]==='"'?(s+='"',a++):n=!n:r===this.csvDelimiter&&!n?(t.push(s),s=""):s+=r}return t.push(s),t}async readExcelFile(e){if(!pt)throw new Error("ReaderFactory module not available");e?.("Reading Excel file...");let t=pt.create(this.filePath);await t.open(this.filePath);let s=[],n=r=>{if(r==null)return"";if(r instanceof Date){let i=o=>o<10?"0"+o:o;return`${r.getFullYear()}-${i(r.getMonth()+1)}-${i(r.getDate())} ${i(r.getHours())}:${i(r.getMinutes())}:${i(r.getSeconds())}`}return String(r)},a=0;for(;await t.read();){let r=[],i=t._currentRow;if(i&&Array.isArray(i))for(let o=0;o<i.length;o++)r.push(n(i[o]));s.push(r),a++,a%1e4===0&&e?.(`Processed ${a.toLocaleString()} rows...`)}return typeof t.close=="function"&&await t.close(),e?.(`Excel file loaded: ${s.length} rows`),s}async analyzeDataTypes(e){e?.("Analyzing data types...");let t;if(this.isExcelFile)this.excelData=await this.readExcelFile(e),t=this.excelData;else{this.detectCsvDelimiter();let n=Q.readFileSync(this.filePath,"utf-8");n.startsWith("\uFEFF")&&(n=n.slice(1));let a=n.split(/\r?\n/);t=[];for(let r of a)r.trim()&&t.push(this.parseCsvLine(r))}if(!t||t.length===0)throw new Error("No data found in file");let s=[];this.sqlHeaders=t[0].map(n=>this.cleanColumnName(n||"COLUMN"));for(let n=0;n<t[0].length;n++)s.push(new ve);for(let n=1;n<t.length;n++){let a=t[n];for(let r=0;r<a.length;r++)r<s.length&&a[r]&&a[r].trim()&&s[r].refreshCurrentType(a[r].trim());n%1e4===0&&e?.(`Analyzed ${n.toLocaleString()} rows...`)}return this.rowsCount=t.length-1,e?.(`Analysis complete: ${this.rowsCount.toLocaleString()} rows`),this.dataTypes=s,s}escapeValue(e){let t=String(e).trim();for(let s of this.valuesToEscape)t=t.split(s).join(`${this.escapechar}${s}`);return t}formatValue(e,t){let s=this.escapeValue(e);return t<this.dataTypes.length&&this.dataTypes[t].currentType.dbType==="DATETIME"&&(s=s.replace("T"," ")),s}generateCreateTableSql(){let e=[];for(let s=0;s<this.sqlHeaders.length;s++){let n=this.sqlHeaders[s],a=this.dataTypes[s];e.push(`        ${n} ${a.currentType.toString()}`)}let t=this.logDir.replace(/\\/g,"/");return`CREATE TABLE ${this.targetTable} AS 
(
    SELECT * FROM EXTERNAL '${this.pipeName}'
    (
${e.join(`,
`)}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${this.delimiterPlain}'
        RecordDelim '${this.recordDelimPlain}'
        ESCAPECHAR '${this.escapechar}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${t}'
    )
) DISTRIBUTE ON RANDOM;`}async createDataFile(e){let t=le.join(this.logDir,`netezza_import_data_${Math.floor(Math.random()*1e3)}.txt`);e?.(`Creating temporary data file: ${t}`);try{let s;if(this.isExcelFile){if(!this.excelData||this.excelData.length===0)throw new Error("Excel data not loaded. Call analyzeDataTypes first.");s=this.excelData.slice(1)}else{let a=Q.readFileSync(this.filePath,"utf-8");a.startsWith("\uFEFF")&&(a=a.slice(1));let r=a.split(/\r?\n/);s=[];let i=!0;for(let o of r)if(o.trim()){if(i){i=!1;continue}s.push(this.parseCsvLine(o))}}let n=[];for(let a=0;a<s.length;a++){let i=s[a].map((o,h)=>this.formatValue(o||"",h));n.push(i.join(this.delimiter)),(a+1)%1e4===0&&e?.(`Processed ${(a+1).toLocaleString()} rows...`)}return Q.writeFileSync(t,n.join(this.recordDelim),"utf-8"),this.pipeName=t.replace(/\\/g,"/"),e?.(`Data file created: ${this.pipeName}`),t}catch(s){throw new Error(`Error creating data file: ${s.message}`)}}getRowsCount(){return this.rowsCount}getSqlHeaders(){return this.sqlHeaders}getCsvDelimiter(){return this.csvDelimiter}}});var en={};ie(en,{ClipboardDataProcessor:()=>Ve,importClipboardDataToNetezza:()=>Vn});function Hn(u){let e=String(u).trim();return e=e.replace(/[^0-9a-zA-Z]+/g,"_").toUpperCase(),(!e||/^\d/.test(e))&&(e="COL_"+e),e}function Wn(u,e,t){let s=String(u).trim();for(let n of t)s=s.split(n).join(`${e}${n}`);return s}function qn(u,e,t,s,n){let a=Wn(u,s,n);return e<t.length&&t[e].currentType.dbType==="DATETIME"&&(a=a.replace("T"," ")),a}async function Vn(u,e,t,s,n){let a=Date.now(),r=null;try{if(!u)return{success:!1,message:"Target table name is required"};if(!e)return{success:!1,message:"Connection string is required"};n?.("Starting clipboard import process..."),n?.(`  Target table: ${u}`),n?.(`  Format preference: ${t||"auto-detect"}`);let i=new Ve,[o,h]=await i.processClipboardData(t,n);if(!o||!o.length)return{success:!1,message:"No data found in clipboard"};if(o.length<2)return{success:!1,message:"Clipboard data must contain at least headers and one data row"};n?.(`  Detected format: ${h}`),n?.(`  Rows: ${o.length}`),n?.(`  Columns: ${o[0].length}`);let c=o[0].map(J=>Hn(J)),x=o.slice(1);n?.("Analyzing clipboard data types...");let p=c.map(()=>new ve);for(let J=0;J<x.length;J++){let de=x[J];for(let te=0;te<de.length;te++)te<p.length&&de[te]&&de[te].trim()&&p[te].refreshCurrentType(de[te].trim());(J+1)%1e3===0&&n?.(`Analyzed ${(J+1).toLocaleString()} rows...`)}n?.(`Analysis complete: ${x.length.toLocaleString()} data rows`);let m=gt.join(require("os").tmpdir(),"netezza_clipboard_logs");ce.existsSync(m)||ce.mkdirSync(m,{recursive:!0});let g="	",y="\\t",w=`
`,A="\\n",M="\\",D=[M,w,"\r",g];r=gt.join(m,`netezza_clipboard_import_${Math.floor(Math.random()*1e3)}.txt`),n?.(`Creating temporary data file: ${r}`);let L=[];for(let J=0;J<x.length;J++){let te=x[J].map((Je,Xe)=>qn(Je,Xe,p,M,D));L.push(te.join(g)),(J+1)%1e3===0&&n?.(`Processed ${(J+1).toLocaleString()} rows...`)}ce.writeFileSync(r,L.join(w),"utf-8");let z=r.replace(/\\/g,"/");n?.(`Data file created: ${z}`);let _=[];for(let J=0;J<c.length;J++)_.push(`        ${c[J]} ${p[J].currentType.toString()}`);let k=m.replace(/\\/g,"/"),H=`CREATE TABLE ${u} AS 
(
    SELECT * FROM EXTERNAL '${z}'
    (
${_.join(`,
`)}
    )
    USING
    (
        REMOTESOURCE 'odbc'
        DELIMITER '${y}'
        RecordDelim '${A}'
        ESCAPECHAR '${M}'
        NULLVALUE ''
        ENCODING 'utf-8'
        TIMESTYLE '24HOUR'
        SKIPROWS 0
        MAXERRORS 10
        LOGDIR '${k}'
    )
) DISTRIBUTE ON RANDOM;`;if(n?.("Generated SQL:"),n?.(H),n?.("Connecting to Netezza..."),!ft)throw new Error("ODBC module not available");let Y=await ft.connect(e);try{n?.("Executing CREATE TABLE with EXTERNAL clipboard data..."),await Y.query(H),n?.("Clipboard import completed successfully")}finally{await Y.close()}let ne=(Date.now()-a)/1e3;return{success:!0,message:"Clipboard import completed successfully",details:{targetTable:u,format:h,rowsProcessed:x.length,rowsInserted:x.length,processingTime:`${ne.toFixed(1)}s`,columns:c.length,detectedDelimiter:g}}}catch(i){let o=(Date.now()-a)/1e3;return{success:!1,message:`Clipboard import failed: ${i.message}`,details:{processingTime:`${o.toFixed(1)}s`}}}finally{if(r&&ce.existsSync(r))try{ce.unlinkSync(r),n?.("Temporary clipboard data file cleaned up")}catch(i){n?.(`Warning: Could not clean up temp file: ${i.message}`)}}}var ce,gt,Zt,ft,Ve,tn=oe(()=>{"use strict";ce=U(require("fs")),gt=U(require("path")),Zt=U(require("vscode"));mt();try{ft=require("odbc")}catch{console.error("ODBC module not available")}Ve=class{constructor(){this.processedData=[]}processXmlSpreadsheet(e,t){t?.("Processing XML Spreadsheet data...");let s=[],n=0,a=[],r=0,i=e.match(/ExpandedColumnCount="(\d+)"/);i&&(n=parseInt(i[1]),t?.(`Table has ${n} columns`));let o=e.match(/ExpandedRowCount="(\d+)"/);o&&t?.(`Table has ${o[1]} rows`);let h=/<Row[^>]*>([\s\S]*?)<\/Row>/gi,c;for(;(c=h.exec(e))!==null;){let x=c[1];a=new Array(n).fill("");let p=/<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*>[\s\S]*?<Data[^>]*>([^<]*)<\/Data>[\s\S]*?<\/Cell>|<Cell(?:\s+[^>]*ss:Index="(\d+)")?[^>]*\/>/gi,m,g=0,y=/<Cell(?:[^>]*ss:Index="(\d+)")?[^>]*>(?:[\s\S]*?<Data[^>]*(?:\s+ss:Type="([^"]*)")?[^>]*>([^<]*)<\/Data>)?[\s\S]*?<\/Cell>/gi;for(;(m=y.exec(x))!==null;){m[1]&&(g=parseInt(m[1])-1);let w=m[2]||"",A=m[3]||"";w==="Boolean"&&(A=A==="0"?"False":"True"),g<n&&(a[g]=A),g++}a.some(w=>w.trim())&&s.push([...a]),r++,r%1e4===0&&t?.(`Analyzed ${r.toLocaleString()} rows...`)}return t?.(`XML processing complete: ${s.length} rows, ${n} columns`),this.processedData=s,s}processTextData(e,t){if(t?.("Processing text data..."),!e.trim())return[];let s=e.split(`
`);for(;s.length&&!s[s.length-1].trim();)s.pop();if(!s.length)return[];let n=["	",",",";","|"],a={};for(let h of n){let c=[];for(let x of s.slice(0,Math.min(5,s.length)))if(x.trim()){let p=x.split(h);c.push(p.length)}if(c.length){let x=c.reduce((m,g)=>m+g,0)/c.length,p=c.reduce((m,g)=>m+Math.pow(g-x,2),0)/c.length;a[h]=[x,-p]}}let r="	";Object.keys(a).length&&(r=Object.keys(a).reduce((h,c)=>{let[x,p]=a[h]||[0,0],[m,g]=a[c];return m>x||m===x&&g>p?c:h},"	")),t?.(`Auto-detected delimiter: '${r==="	"?"\\t":r}'`);let i=[],o=0;for(let h of s)if(h.trim()){let c=h.split(r).map(x=>x.trim());i.push(c),o=Math.max(o,c.length)}for(let h of i)for(;h.length<o;)h.push("");return t?.(`Text processing complete: ${i.length} rows, ${o} columns`),this.processedData=i,i}async getClipboardText(){return await Zt.env.clipboard.readText()}async processClipboardData(e,t){t?.("Getting clipboard data...");let s=await this.getClipboardText();if(!s)throw new Error("No data found in clipboard");t?.(`Data size: ${s.length} characters`);let n="TEXT";e==="XML Spreadsheet"||!e&&s.includes("<Workbook")&&s.includes("<Worksheet")?n="XML Spreadsheet":e==="TEXT"&&(n="TEXT"),t?.(`Detected format: ${n}`);let a;return n==="XML Spreadsheet"?a=this.processXmlSpreadsheet(s,t):a=this.processTextData(s,t),t?.(`Processed ${a.length} rows`),a.length&&t?.(`Columns per row: ${a[0].length}`),[a,n]}}});var jn={};ie(jn,{activate:()=>Xn,deactivate:()=>Yn});module.exports=xn(jn);var l=U(require("vscode"));fe();Ye();var Z=U(require("vscode")),Le=class u{constructor(e,t,s){this.extensionUri=t;this.connectionManager=s;this._disposables=[];this._panel=e,this._panel.onDidDispose(()=>this.dispose(),null,this._disposables),this._panel.webview.html=this._getHtmlForWebview(this._panel.webview),this._panel.webview.onDidReceiveMessage(async n=>{switch(n.command){case"save":try{await this.connectionManager.saveConnection(n.data),Z.window.showInformationMessage(`Connection '${n.data.name}' saved and activated!`),this.sendConnectionsToWebview()}catch(a){Z.window.showErrorMessage(`Error saving: ${a.message}`)}return;case"delete":try{await Z.window.showWarningMessage(`Are you sure you want to delete '${n.name}'?`,{modal:!0},"Yes","No")==="Yes"&&(await this.connectionManager.deleteConnection(n.name),Z.window.showInformationMessage(`Connection '${n.name}' deleted.`),this.sendConnectionsToWebview())}catch(a){Z.window.showErrorMessage(`Error deleting: ${a.message}`)}return;case"loadConnections":this.sendConnectionsToWebview();return}},null,this._disposables)}async sendConnectionsToWebview(){let e=await this.connectionManager.getConnections(),t=this.connectionManager.getActiveConnectionName();await this._panel.webview.postMessage({command:"updateConnections",connections:e,activeName:t})}static createOrShow(e,t){let s=Z.window.activeTextEditor?Z.window.activeTextEditor.viewColumn:void 0;if(u.currentPanel){u.currentPanel._panel.reveal(s);return}let n=Z.window.createWebviewPanel("netezzaLogin","Connect to Netezza",s||Z.ViewColumn.One,{enableScripts:!0,retainContextWhenHidden:!0});u.currentPanel=new u(n,e,t)}dispose(){for(u.currentPanel=void 0,this._panel.dispose();this._disposables.length;){let e=this._disposables.pop();e&&e.dispose()}}_getHtmlForWebview(e){let t=e.asWebviewUri(Z.Uri.joinPath(this.extensionUri,"netezza_icon64.png"));return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Connect to Netezza</title>
            <style>
                :root {
                    --container-paddding: 20px;
                    --input-padding-vertical: 6px;
                    --input-padding-horizontal: 8px;
                    --input-margin-vertical: 4px;
                    --input-margin-horizontal: 0;
                }
                body {
                    font-family: var(--vscode-font-family);
                    padding: 0;
                    margin: 0;
                    color: var(--vscode-foreground);
                    background-color: var(--vscode-editor-background);
                    display: flex;
                    height: 100vh;
                    overflow: hidden;
                }
                
                /* Sidebar */
                .sidebar {
                    width: 260px;
                    background-color: var(--vscode-sideBar-background);
                    border-right: 1px solid var(--vscode-panel-border);
                    display: flex;
                    flex-direction: column;
                    flex-shrink: 0;
                    user-select: none;
                }
                .sidebar-header {
                    padding: 10px 15px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    background-color: var(--vscode-sideBarSectionHeader-background);
                }
                .sidebar-title {
                    font-weight: bold;
                    font-size: 11px;
                    text-transform: uppercase;
                    color: var(--vscode-sideBarTitle-foreground);
                }
                .connection-list {
                    flex: 1;
                    overflow-y: auto;
                    padding: 0;
                }
                .connection-item {
                    padding: 8px 15px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    border-left: 3px solid transparent;
                    color: var(--vscode-sideBar-foreground);
                }
                .connection-item:hover {
                    background-color: var(--vscode-list-hoverBackground);
                }
                .connection-item.active {
                    background-color: var(--vscode-list-activeSelectionBackground);
                    color: var(--vscode-list-activeSelectionForeground);
                    border-left-color: var(--vscode-focusBorder);
                }
                .connection-item .name {
                    flex: 1;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }
                .connection-item .status {
                    font-size: 0.8em;
                    margin-left: 5px;
                    opacity: 0.7;
                }

                /* Main Content */
                .main {
                    flex: 1;
                    padding: 40px;
                    overflow-y: auto;
                    display: flex;
                    justify-content: center;
                    align-items: flex-start;
                }
                .form-container {
                    width: 100%;
                    max-width: 500px;
                    background-color: var(--vscode-editorWidget-background);
                    border: 1px solid var(--vscode-widget-border);
                    padding: 30px;
                    border-radius: 4px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                
                h2 {
                    margin-top: 0;
                    margin-bottom: 25px;
                    font-size: 1.4em;
                    font-weight: 500;
                    padding-bottom: 10px;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .form-group {
                    margin-bottom: 18px;
                }
                .form-row {
                    display: flex;
                    gap: 15px;
                }
                .form-col {
                    flex: 1;
                }

                label {
                    display: block;
                    margin-bottom: 6px;
                    font-weight: 600;
                    font-size: 12px;
                    color: var(--vscode-input-placeholderForeground);
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }

                input, select {
                    width: 100%;
                    padding: 8px 10px;
                    box-sizing: border-box;
                    background: var(--vscode-input-background);
                    color: var(--vscode-input-foreground);
                    border: 1px solid var(--vscode-input-border);
                    border-radius: 2px;
                    font-family: inherit;
                    font-size: 13px;
                }
                input:focus, select:focus {
                    border-color: var(--vscode-focusBorder);
                    outline: 1px solid var(--vscode-focusBorder);
                }
                
                /* Buttons */
                .actions {
                    margin-top: 30px;
                    display: flex;
                    gap: 12px;
                    padding-top: 20px;
                    border-top: 1px solid var(--vscode-panel-border);
                }
                button {
                    background: var(--vscode-button-background);
                    color: var(--vscode-button-foreground);
                    padding: 8px 16px;
                    border: none;
                    cursor: pointer;
                    border-radius: 2px;
                    font-size: 13px;
                    font-weight: 500;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                button:hover {
                    background: var(--vscode-button-hoverBackground);
                }
                button.secondary {
                    background: var(--vscode-button-secondaryBackground);
                    color: var(--vscode-button-secondaryForeground);
                }
                button.secondary:hover {
                    background: var(--vscode-button-secondaryHoverBackground);
                }
                button.danger {
                    background: var(--vscode-errorForeground);
                    color: white;
                }
                button.icon-btn {
                    padding: 4px;
                    background: transparent;
                    color: var(--vscode-icon-foreground);
                }
                button.icon-btn:hover {
                    background: var(--vscode-toolbar-hoverBackground);
                }

                .icon-img {
                    width: 16px;
                    height: 16px;
                    object-fit: contain;
                }
                .logo-header {
                    width: 32px;
                    height: 32px;
                    margin-right: 10px;
                }

            </style>
        </head>
        <body>
            <div class="sidebar">
                <div class="sidebar-header">
                    <span class="sidebar-title">Saved Connections</span>
                    <button class="icon-btn" id="btnNew" title="New Connection">
                        <svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M14 7v1H8v6H7V8H1V7h6V1h1v6h6z"/></svg>
                    </button>
                </div>
                <div id="connectionList" class="connection-list"></div>
            </div>
            
            <div class="main">
                <div class="form-container">
                    <h2 id="formTitle">
                        <img src="${t}" class="logo-header" />
                        New Connection
                    </h2>
                    
                    <div class="form-group">
                        <label for="name">Connection Name</label>
                        <input type="text" id="name" placeholder="Friendly name (e.g. Production)">
                    </div>

                    <div class="form-group">
                        <label for="dbType">
                            Database Type 
                            <img src="${t}" class="icon-img" />
                        </label>
                        <select id="dbType">
                            <option value="NetezzaSQL">NetezzaSQL</option>
                        </select>
                    </div>

                    <div class="form-row">
                        <div class="form-col">
                            <div class="form-group">
                                <label for="host">Host</label>
                                <input type="text" id="host" placeholder="Hostname or IP">
                            </div>
                        </div>
                        <div class="form-col" style="flex: 0 0 80px;">
                            <div class="form-group">
                                <label for="port">Port</label>
                                <input type="number" id="port" value="5480">
                            </div>
                        </div>
                    </div>

                    <div class="form-group">
                        <label for="database">
                            Database
                        </label>
                        <input type="text" id="database" placeholder="Database name" value="system">
                    </div>

                    <div class="form-row">
                         <div class="form-col">
                            <div class="form-group">
                                <label for="user">User</label>
                                <input type="text" id="user" placeholder="Username">
                            </div>
                         </div>
                         <div class="form-col">
                            <div class="form-group">
                                <label for="password">Password</label>
                                <input type="password" id="password" placeholder="Password">
                            </div>
                         </div>
                    </div>
                    
                    <div class="actions">
                        <button id="btnSave" onclick="save()">Save & Connect</button>
                        <button id="btnDelete" class="danger" onclick="del()" style="display: none;">Delete</button>
                    </div>
                </div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();
                let connections = [];
                let activeName = null;
                let currentEditName = null;
                const iconSrc = "${t}";

                // Load initial data
                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.command) {
                        case 'updateConnections':
                            connections = message.connections;
                            activeName = message.activeName;
                            renderList();
                            break;
                    }
                });
                
                vscode.postMessage({ command: 'loadConnections' });

                document.getElementById('btnNew').addEventListener('click', () => {
                    clearForm();
                });

                function renderList() {
                    const list = document.getElementById('connectionList');
                    list.innerHTML = '';
                    
                    connections.forEach(conn => {
                        const div = document.createElement('div');
                        div.className = 'connection-item';
                        if (conn.name === currentEditName) {
                             div.classList.add('active');
                        }
                        
                        div.innerHTML = \`<span class="name"><img src="\${iconSrc}" class="icon-img"> \${conn.name}</span>\`;
                        if (conn.name === activeName) {
                            div.innerHTML += \`<span class="status">\u25CF</span>\`;
                            div.title = 'Active Connection';
                        }
                        
                        div.onclick = () => loadForm(conn);
                        list.appendChild(div);
                    });
                }

                function loadForm(conn) {
                    currentEditName = conn.name;
                    document.getElementById('formTitle').innerHTML = \`<img src="\${iconSrc}" class="logo-header" /> Edit Connection\`;
                    document.getElementById('name').value = conn.name;
                    document.getElementById('dbType').value = conn.dbType || 'NetezzaSQL';
                    document.getElementById('host').value = conn.host;
                    document.getElementById('port').value = conn.port;
                    document.getElementById('database').value = conn.database;
                    document.getElementById('user').value = conn.user;
                    document.getElementById('password').value = conn.password || ''; 
                    
                    document.getElementById('btnDelete').style.display = 'block';
                    renderList();
                }

                function clearForm() {
                    currentEditName = null;
                    document.getElementById('formTitle').innerHTML = \`<img src="\${iconSrc}" class="logo-header" /> New Connection\`;
                    document.getElementById('name').value = '';
                    document.getElementById('dbType').value = 'NetezzaSQL';
                    document.getElementById('host').value = '';
                    document.getElementById('port').value = '5480';
                    document.getElementById('database').value = 'system';
                    document.getElementById('user').value = '';
                    document.getElementById('password').value = '';
                    
                    document.getElementById('btnDelete').style.display = 'none';
                    renderList();
                }

                function save() {
                    const name = document.getElementById('name').value;
                    if (!name) {
                        return; // Add validation UI?
                    }
                    
                    const data = {
                        name: name,
                        dbType: document.getElementById('dbType').value,
                        host: document.getElementById('host').value,
                        port: parseInt(document.getElementById('port').value),
                        database: document.getElementById('database').value,
                        user: document.getElementById('user').value,
                        password: document.getElementById('password').value
                    };
                    
                    vscode.postMessage({
                        command: 'save',
                        data: data
                    });
                }

                function del() {
                    if (currentEditName) {
                        vscode.postMessage({
                            command: 'delete',
                            name: currentEditName
                        });
                    }
                }
            </script>
        </body>
        </html>`}};Pe();var V=U(require("vscode")),Se=class{constructor(e){this._resultsMap=new Map;this._pinnedSources=new Set;this._pinnedResults=new Map;this._resultIdCounter=0;this._extensionUri=e}static{this.viewType="netezza.results"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[V.Uri.joinPath(this._extensionUri,"media")]},e.webview.html=this._getHtmlForWebview(),e.webview.onDidReceiveMessage(n=>{switch(n.command){case"exportCsv":this.exportCsv(n.data);return;case"openInExcel":this.openInExcel(n.data,n.sql);return;case"copyAsExcel":this.copyAsExcel(n.data,n.sql);return;case"switchSource":this._activeSourceUri=n.sourceUri,this._updateWebview();return;case"togglePin":this._pinnedSources.has(n.sourceUri)?this._pinnedSources.delete(n.sourceUri):this._pinnedSources.add(n.sourceUri),this._updateWebview();return;case"toggleResultPin":this._toggleResultPin(n.sourceUri,n.resultSetIndex);return;case"switchToPinnedResult":this._switchToPinnedResult(n.resultId);return;case"unpinResult":this._pinnedResults.delete(n.resultId),this._updateWebview();return;case"closeSource":this.closeSource(n.sourceUri);return;case"copyToClipboard":V.env.clipboard.writeText(n.text),V.window.showInformationMessage("Copied to clipboard");return;case"info":V.window.showInformationMessage(n.text);return;case"error":V.window.showErrorMessage(n.text);return}})}setActiveSource(e){this._resultsMap.has(e)&&this._activeSourceUri!==e&&(this._activeSourceUri=e,this._updateWebview())}updateResults(e,t,s=!1){this._resultsMap.has(t)||this._pinnedSources.add(t);let n=[];Array.isArray(e)?n=e:n=[e];let a=this._resultsMap.get(t)||[],r=Array.from(this._pinnedResults.entries()).filter(([h,c])=>c.sourceUri===t).sort((h,c)=>h[1].resultSetIndex-c[1].resultSetIndex),i=[],o=[];r.forEach(([h,c])=>{c.resultSetIndex<a.length&&(i.push(a[c.resultSetIndex]),o.push([h,c]))}),i.push(...n),o.forEach(([h,c],x)=>{let p=this._pinnedResults.get(h);p&&(p.resultSetIndex=x)}),s||Array.from(this._resultsMap.keys()).filter(c=>c!==t&&!this._pinnedSources.has(c)).forEach(c=>{this._resultsMap.delete(c),Array.from(this._pinnedResults.entries()).filter(([p,m])=>m.sourceUri===c).map(([p,m])=>p).forEach(p=>this._pinnedResults.delete(p))}),this._resultsMap.set(t,i),this._activeSourceUri=t,this._view?(this._updateWebview(),this._view.show?.(!0)):V.window.showInformationMessage('Query completed. Please open "Query Results" panel to view data.')}_updateWebview(){this._view&&(this._view.webview.html=this._getHtmlForWebview())}_toggleResultPin(e,t){let s=Array.from(this._pinnedResults.entries()).find(([n,a])=>a.sourceUri===e&&a.resultSetIndex===t);if(s)this._pinnedResults.delete(s[0]);else{let n=`result_${++this._resultIdCounter}`,a=Date.now(),i=`${e.split(/[\\/]/).pop()||e} - Result ${t+1}`;this._pinnedResults.set(n,{sourceUri:e,resultSetIndex:t,timestamp:a,label:i})}this._updateWebview()}_switchToPinnedResult(e){let t=this._pinnedResults.get(e);t&&(this._activeSourceUri=t.sourceUri,this._updateWebview(),this._view&&this._view.webview.postMessage({command:"switchToResultSet",resultSetIndex:t.resultSetIndex}))}async exportCsv(e){let t=await V.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export"});t&&(await V.workspace.fs.writeFile(t,Buffer.from(e)),V.window.showInformationMessage(`Results exported to ${t.fsPath}`))}async openInExcel(e,t){V.commands.executeCommand("netezza.exportCurrentResultToXlsbAndOpen",e,t)}async copyAsExcel(e,t){V.commands.executeCommand("netezza.copyCurrentResultToXlsbClipboard",e,t)}closeSource(e){if(this._resultsMap.has(e)){if(this._resultsMap.delete(e),this._pinnedSources.delete(e),Array.from(this._pinnedResults.entries()).filter(([s,n])=>n.sourceUri===e).map(([s,n])=>s).forEach(s=>this._pinnedResults.delete(s)),this._activeSourceUri===e){let s=Array.from(this._resultsMap.keys());this._activeSourceUri=s.length>0?s[0]:void 0}this._updateWebview()}}_getHtmlForWebview(){if(!this._view)return"";let{scriptUri:e,virtualUri:t,mainScriptUri:s,styleUri:n,workerUri:a}=this._getScriptUris(),r=this._prepareViewData();return this._buildHtmlDocument(e,t,s,n,r,a)}_getScriptUris(){return{scriptUri:this._view.webview.asWebviewUri(V.Uri.joinPath(this._extensionUri,"media","tanstack-table-core.js")),virtualUri:this._view.webview.asWebviewUri(V.Uri.joinPath(this._extensionUri,"media","tanstack-virtual-core.js")),mainScriptUri:this._view.webview.asWebviewUri(V.Uri.joinPath(this._extensionUri,"media","resultPanel.js")),workerUri:this._view.webview.asWebviewUri(V.Uri.joinPath(this._extensionUri,"media","searchWorker.js")),styleUri:this._view.webview.asWebviewUri(V.Uri.joinPath(this._extensionUri,"media","resultPanel.css"))}}_prepareViewData(){let e=Array.from(this._resultsMap.keys()),t=Array.from(this._pinnedSources),s=Array.from(this._pinnedResults.entries()).map(([i,o])=>({id:i,...o})),n=this._activeSourceUri&&this._resultsMap.has(this._activeSourceUri)?this._activeSourceUri:e.length>0?e[0]:null,a=n?this._resultsMap.get(n):[],r=(i,o)=>typeof o=="bigint"?o>=Number.MIN_SAFE_INTEGER&&o<=Number.MAX_SAFE_INTEGER?Number(o):o.toString():o;return{sourcesJson:JSON.stringify(e),pinnedSourcesJson:JSON.stringify(t),pinnedResultsJson:JSON.stringify(s),activeSourceJson:JSON.stringify(n),resultSetsJson:JSON.stringify(a,r)}}_buildHtmlDocument(e,t,s,n,a,r){let i=this._view.webview.cspSource,o={eye:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 3c-3 0-6 2.5-6 5s3 5 6 5 6-2.5 6-5-3-5-6-5zm0 9c-2.5 0-4.5-2-4.5-4S5.5 4 8 4s4.5 2 4.5 4-2 4.5-4.5 4.5z"/><circle cx="8" cy="8" r="2"/></svg>',excel:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M6 3h8v10H6V3zm-1 0H3v10h2V3zm-2-1h9a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3a1 1 0 0 1 1-1z"/><path d="M6 6h8v1H6V6zm0 2h8v1H6V8zm0 2h8v1H6v-1z"/></svg>',copy:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M4 4h7v2H4V4zm0 4h7v2H4V8zm0 4h7v2H4v-2zM2 1h12v14H2V1zm1 1v12h10V2H3z"/></svg>',csv:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M13 2H6L2 6v8a1 1 0 0 0 1 1h10a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1zm-1 11H4V7h3V4h5v9z"/></svg>',checkAll:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M13.485 1.929l1.414 1.414-9.9 9.9-4.243-4.242 1.415-1.415 2.828 2.829z"/></svg>',clear:'<svg width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="currentColor"><path d="M8 7.293l4.146-4.147.708.708L8.707 8l4.147 4.146-.708.708L8 8.707l-4.146 4.147-.708-.708L7.293 8 3.146 3.854l.708-.708L8 7.293z"/></svg>'};return`<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src ${i} 'unsafe-inline'; worker-src ${i} blob:; connect-src ${i}; style-src ${i} 'unsafe-inline';">
            <title>Query Results</title>
            <script src="${e}"></script>
            <script src="${t}"></script>
            <link rel="stylesheet" href="${n}">
        </head>
        <body>
            <div id="sourceTabs" class="source-tabs"></div>
            <div id="resultSetTabs" class="result-set-tabs" style="display: none;"></div>
            
            <div class="controls">
                <input type="text" id="globalFilter" placeholder="Filter..." onkeyup="onFilterChanged()" style="background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 4px;">
                <button onclick="toggleRowView()" title="Toggle Row View">${o.eye} Row View</button>
                <button onclick="openInExcel()" title="Open results in Excel">${o.excel} Excel</button>
                <button onclick="copyAsExcel()" title="Copy results as Excel to clipboard">${o.excel} Excel Copy</button>
                <button onclick="exportToCsv()" title="Export results to CSV">${o.csv} CSV</button>
                <div style="width: 1px; height: 16px; background: var(--vscode-panel-border); margin: 0 4px;"></div>
                <button onclick="selectAll()" title="Select all rows">${o.checkAll} Select All</button>
                <button onclick="copySelection(false)" title="Copy selected cells to clipboard">${o.copy} Copy</button>
                <button onclick="copySelection(true)" title="Copy selected cells with headers">${o.copy} Copy w/ Headers</button>
                <button onclick="clearAllFilters()" title="Clear all column filters">${o.clear} Clear Filters</button>
                <span id="rowCountInfo" style="margin-left: auto; font-size: 12px; opacity: 0.8;"></span>
            </div>

            <div id="groupingPanel" class="grouping-panel" ondragover="onDragOverGroup(event)" ondragleave="onDragLeaveGroup(event)" ondrop="onDropGroup(event)">
                <span style="opacity: 0.5;">Drag headers here to group</span>
            </div>

            <div id="mainSplitView" class="main-split-view">
                <div id="gridContainer"></div>
                <div id="rowViewPanel" class="row-view-panel">
                    <div class="row-view-header">
                        <span>Row Details & Comparison</span>
                        <span class="row-view-close" onclick="toggleRowView()">\xD7</span>
                    </div>
                    <div id="rowViewContent" class="row-view-content">
                        <div class="row-view-placeholder">Select 1 or 2 rows to view details or compare</div>
                    </div>
                </div>
            </div>
            
            <script>
                const vscode = acquireVsCodeApi();
                window.sources = ${a.sourcesJson};
                window.pinnedSources = new Set(${a.pinnedSourcesJson});
                window.pinnedResults = ${a.pinnedResultsJson};
                window.activeSource = ${a.activeSourceJson};
                window.resultSets = ${a.resultSetsJson};
                
                let grids = [];
                let activeGridIndex = window.resultSets && window.resultSets.length > 0 ? window.resultSets.length - 1 : 0;
                const workerUri = "${r}";
            </script>
            <script src="${s}"></script>
            <script>
                // Initialize on load
                init();
            </script>
        </body>
        </html>`}};var B=U(require("vscode"));fe();var Be=class{constructor(e,t,s){this.context=e;this.metadataCache=t;this.connectionManager=s}async provideCompletionItems(e,t,s,n){let a=e.getText(),r=this.stripComments(a),i=this.parseLocalDefinitions(r),o=e.lineAt(t).text.substr(0,t.character),h=o.toUpperCase(),c=t.line>0?e.lineAt(t.line-1).text:"",x=c.toUpperCase(),p=this.connectionManager.getConnectionForExecution(e.uri.toString());if(p||(p=this.connectionManager.getActiveConnectionName()||void 0),p&&!this.metadataCache.hasConnectionPrefetchTriggered(p)&&this.metadataCache.triggerConnectionPrefetch(p,D=>F(this.context,D,!0,p,this.connectionManager)),/(FROM|JOIN)\s+$/.test(h)){let D=await this.getDatabases(p);return[...i.map(z=>{let _=new B.CompletionItem(z.name,B.CompletionItemKind.Class);return _.detail=z.type,_}),...D]}if(/(?:FROM|JOIN)\s*$/i.test(c)&&/^\s*[a-zA-Z0-9_]*$/.test(o)){let D=await this.getDatabases(p);return[...i.map(z=>{let _=new B.CompletionItem(z.name,B.CompletionItemKind.Class);return _.detail=z.type,_}),...D]}let m=o.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\s*$/i);if(m){let D=m[1],L=await this.getSchemas(p,D);return new B.CompletionList(L,!1)}let g=o.match(/^\s*([a-zA-Z0-9_]+)\.\s*$/i);if(g&&/(?:FROM|JOIN)\s*$/i.test(c)){let D=g[1],L=await this.getSchemas(p,D);return new B.CompletionList(L,!1)}let y=o.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.$/i);if(y){let D=y[1],L=y[2];return this.getTables(p,D,L)}let w=o.match(/^\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.\s*$/i);if(w&&/(?:FROM|JOIN)\s*$/i.test(c)){let D=w[1],L=w[2];return this.getTables(p,D,L)}let A=o.match(/(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\.\.$/i);if(A){let D=A[1];return this.getTables(p,D,void 0)}let M=o.match(/^\s*([a-zA-Z0-9_]+)\.\.\s*$/i);if(M&&/(?:FROM|JOIN)\s*$/i.test(c)){let D=M[1];return this.getTables(p,D,void 0)}if(o.trim().endsWith(".")){let D=o.trim().split(/[\s.]+/),L=o.match(/([a-zA-Z0-9_]+)\.$/);if(L){let z=L[1],_=this.findAlias(r,z);if(_){let H=i.find(Y=>Y.name.toUpperCase()===_.table.toUpperCase());return H?H.columns.map(Y=>{let ne=new B.CompletionItem(Y,B.CompletionItemKind.Field);return ne.detail="Local Column",ne}):this.getColumns(p,_.db,_.schema,_.table)}let k=i.find(H=>H.name.toUpperCase()===z.toUpperCase());if(k)return k.columns.map(H=>{let Y=new B.CompletionItem(H,B.CompletionItemKind.Field);return Y.detail="Local Column",Y})}}return this.getKeywords()}stripComments(e){let t=e.replace(/--.*$/gm,"");return t=t.replace(/\/\*[\s\S]*?\*\//g,""),t}parseLocalDefinitions(e){let t=[],s=/CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s+AS\s*\(/gi,n;for(;(n=s.exec(e))!==null;){let i=n[1],o=n.index+n[0].length,h=this.extractBalancedParenthesisContent(e,o);if(h){let c=this.extractColumnsFromQuery(h);t.push({name:i,type:"Temp Table",columns:c})}}let a=/\bWITH\s+/gi;for(;(n=a.exec(e))!==null;){let i=n.index+n[0].length;for(;;){let o=/^\s*([a-zA-Z0-9_]+)\s+AS\s*\(/i,h=e.substring(i),c=h.match(o);if(!c)break;let x=c[1],p=i+c[0].length,m=h.indexOf("(",c.index+c[1].length),g=i+m,y=this.extractBalancedParenthesisContent(e,g+1);if(y){let w=this.extractColumnsFromQuery(y);t.push({name:x,type:"CTE",columns:w}),i=g+1+y.length+1;let A=/^\s*,/,M=e.substring(i);if(A.test(M)){let D=M.match(A);i+=D[0].length}else break}else break}}let r=/\bJOIN\s+\(/gi;for(;(n=r.exec(e))!==null;){let i=n.index+n[0].length,o=this.extractBalancedParenthesisContent(e,i);if(o&&/^\s*SELECT\b/i.test(o)){let h=i+o.length+1,x=e.substring(h).match(/^\s+(?:AS\s+)?([a-zA-Z0-9_]+)/i);if(x){let p=x[1],m=this.extractColumnsFromQuery(o);t.push({name:p,type:"Subquery",columns:m})}}}return t}extractBalancedParenthesisContent(e,t){let s=1,n=t;for(;n<e.length;n++)if(e[n]==="("?s++:e[n]===")"&&s--,s===0)return e.substring(t,n);return null}extractColumnsFromQuery(e){let t=e.match(/^\s*SELECT\s+/i);if(!t)return[];let s="",n=0,a=-1,r=t[0].length;for(let h=r;h<e.length;h++)if(e[h]==="("?n++:e[h]===")"&&n--,n===0&&e.substr(h).match(/^\s+FROM\b/i)){a=h;break}a!==-1?s=e.substring(r,a):s=e.substring(r);let i=[],o="";n=0;for(let h=0;h<s.length;h++){let c=s[h];c==="("?n++:c===")"&&n--,c===","&&n===0?(i.push(o.trim()),o=""):o+=c}return o.trim()&&i.push(o.trim()),i.map(h=>{let c=h.match(/\s+AS\s+([a-zA-Z0-9_]+)$/i);if(c)return c[1];let x=h.match(/\s+([a-zA-Z0-9_]+)$/i);if(x)return x[1];let p=h.split(".");return p[p.length-1]})}getKeywords(){return["SELECT","FROM","WHERE","GROUP BY","ORDER BY","LIMIT","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","TABLE","VIEW","DATABASE","JOIN","INNER","LEFT","RIGHT","OUTER","ON","AND","OR","NOT","NULL","IS","IN","BETWEEN","LIKE","AS","DISTINCT","CASE","WHEN","THEN","ELSE","END","WITH","UNION","ALL"].map(t=>{let s=new B.CompletionItem(t,B.CompletionItemKind.Keyword);return s.detail="SQL Keyword",s})}async getDatabases(e){if(!e)return[];let t=this.metadataCache.getDatabases(e);if(t)return t.map(s=>{if(s instanceof B.CompletionItem)return s;let n=new B.CompletionItem(s.label,s.kind);return n.detail=s.detail,n});try{let n=await F(this.context,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0,e,this.connectionManager);if(!n)return[];let r=JSON.parse(n).map(i=>{let o=new B.CompletionItem(i.DATABASE,B.CompletionItemKind.Module);return o.detail="Database",o});return this.metadataCache.setDatabases(e,r),r}catch(s){return console.error(s),[]}}async getSchemas(e,t){if(!e)return[];let s=this.metadataCache.getSchemas(e,t);if(s)return s.map(a=>{if(a instanceof B.CompletionItem)return a;let r=new B.CompletionItem(a.label,a.kind);return r.detail=a.detail,r.insertText=a.insertText,r.sortText=a.sortText,r.filterText=a.filterText,r});let n=B.window.setStatusBarMessage(`Fetching schemas for ${t}...`);try{let a=`SELECT SCHEMA FROM ${t}.._V_SCHEMA ORDER BY SCHEMA`,r=await F(this.context,a,!0,e,this.connectionManager);if(!r)return[];let o=JSON.parse(r).filter(h=>h.SCHEMA!=null&&h.SCHEMA!=="").map(h=>{let c=h.SCHEMA,x=new B.CompletionItem(c,B.CompletionItemKind.Folder);return x.detail=`Schema in ${t}`,x.insertText=c,x.sortText=c,x.filterText=c,x});return this.metadataCache.setSchemas(e,t,o),o}catch(a){return console.error("[SqlCompletion] Error in getSchemas:",a),[]}finally{n.dispose()}}async getTables(e,t,s){if(!e)return[];let n=s?`${t}.${s}`:`${t}..`,a=this.metadataCache.getTables(e,n);if(a)return a.map(o=>{if(o instanceof B.CompletionItem)return o;let h=new B.CompletionItem(o.label,o.kind);return h.detail=o.detail,h.sortText=o.sortText,h});let r=s?`Fetching tables for ${t}.${s}...`:`Fetching tables for ${t}...`,i=B.window.setStatusBarMessage(r);try{let o="";s?o=`SELECT OBJNAME, OBJID FROM ${t}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${t}') AND UPPER(SCHEMA) = UPPER('${s}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`:o=`SELECT OBJNAME, OBJID, SCHEMA FROM ${t}.._V_OBJECT_DATA WHERE UPPER(DBNAME) = UPPER('${t}') AND OBJTYPE='TABLE' ORDER BY OBJNAME`;let h=await F(this.context,o,!0,e,this.connectionManager);if(!h)return[];let c=JSON.parse(h),x=new Map,p=c.map(m=>{let g=new B.CompletionItem(m.OBJNAME,B.CompletionItemKind.Class);return g.detail=s?"Table":`Table (${m.SCHEMA})`,g.sortText=m.OBJNAME,s?x.set(`${t}.${s}.${m.OBJNAME}`,m.OBJID):m.SCHEMA&&x.set(`${t}.${m.SCHEMA}.${m.OBJNAME}`,m.OBJID),g});return this.metadataCache.setTables(e,n,p,x),p}catch(o){return console.error(o),[]}finally{i.dispose()}}async getColumns(e,t,s,n){if(!e)return[];let a,r=t?`${t}..`:"",i=s&&t?`${t}.${s}.${n}`:t?`${t}..${n}`:void 0;i&&(a=this.metadataCache.findTableId(e,i));let o=`${t||"CURRENT"}.${s||""}.${n}`,h=this.metadataCache.getColumns(e,o);if(h)return h.map(x=>{if(x instanceof B.CompletionItem)return x;let p=new B.CompletionItem(x.label,x.kind);return p.detail=x.detail,p});let c=B.window.setStatusBarMessage(`Fetching columns for ${n}...`);try{let x="";if(a)x=`SELECT ATTNAME, FORMAT_TYPE FROM ${r}_V_RELATION_COLUMN WHERE OBJID = ${a} ORDER BY ATTNUM`;else{let y=s?`AND UPPER(O.SCHEMA) = UPPER('${s}')`:"",w=t?`AND UPPER(O.DBNAME) = UPPER('${t}')`:"";x=`
                    SELECT C.ATTNAME, C.FORMAT_TYPE 
                    FROM ${r}_V_RELATION_COLUMN C
                    JOIN ${r}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) = UPPER('${n}') ${y} ${w}
                    ORDER BY C.ATTNUM
                `}let p=await F(this.context,x,!0,e,this.connectionManager);if(!p)return[];let g=JSON.parse(p).map(y=>{let w=new B.CompletionItem(y.ATTNAME,B.CompletionItemKind.Field);return w.detail=y.FORMAT_TYPE,w});if(this.metadataCache.setColumns(e,o,g),t){let y=this.context,w=this.metadataCache;setTimeout(async()=>{try{await w.prefetchColumnsForSchema(e,t,s,A=>F(y,A,!0,e,this.connectionManager))}catch(A){console.error("[SqlCompletion] Background column prefetch error:",A)}},100)}return g}catch(x){return console.error(x),[]}finally{c.dispose()}}findAlias(e,t){let s=new RegExp(`([a-zA-Z0-9_\\.]+) (?:AS\\s+)?${t}\\b`,"gi"),n=new Set(["SELECT","WHERE","GROUP","ORDER","HAVING","LIMIT","ON","AND","OR","NOT","CASE","WHEN","THEN","ELSE","END","JOIN","LEFT","RIGHT","INNER","OUTER","CROSS","FULL","UNION","EXCEPT","INTERSECT","FROM","UPDATE","DELETE","INSERT","INTO","VALUES","SET"]),a;for(;(a=s.exec(e))!==null;){let r=a[1];if(n.has(r.toUpperCase()))continue;let i=r.split(".");return i.length===3?{db:i[0],schema:i[1],table:i[2]}:i.length===2?{table:i[1],schema:i[0]}:{table:i[0]}}return null}};var Ae=U(require("vscode"));fe();var Ie=class{constructor(e,t,s,n){this._extensionUri=e;this.context=t;this.metadataCache=s;this.connectionManager=n;this.currentSearchId=0}static{this.viewType="netezza.search"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"search":await this.search(n.value);break;case"navigate":Ae.commands.executeCommand("netezza.revealInSchema",n);break}})}async search(e){if(!e||e.length<2)return;let t,s=Ae.window.activeTextEditor;if(s&&s.document.languageId==="sql"&&(t=this.connectionManager.getConnectionForExecution(s.document.uri.toString())),t||(t=this.connectionManager.getActiveConnectionName()||void 0),!t){this._view?.webview.postMessage({type:"results",data:[],append:!1});return}let n=++this.currentSearchId,a=Ae.window.setStatusBarMessage(`$(loading~spin) Searching for "${e}"...`),r=new Set;if(this._view){let c=this.metadataCache.search(e,t);if(c.length>0){let x=[];c.forEach(p=>{let m=`${p.name.toUpperCase().trim()}|${p.type.toUpperCase().trim()}|${(p.parent||"").toUpperCase().trim()}`;r.has(m)||(r.add(m),x.push({NAME:p.name,SCHEMA:p.schema,DATABASE:p.database,TYPE:p.type,PARENT:p.parent||"",DESCRIPTION:"Result from Cache",MATCH_TYPE:"NAME",connectionName:t}))}),x.sort((p,m)=>{let g=y=>y==="COLUMN"?2:1;return g(m.TYPE)-g(p.TYPE)?g(p.TYPE)-g(m.TYPE):p.NAME.localeCompare(m.NAME)}),x.length>0&&n===this.currentSearchId&&this._view.webview.postMessage({type:"results",data:x,append:!1})}else n===this.currentSearchId&&this._view.webview.postMessage({type:"results",data:[],append:!1});this.metadataCache.hasAllObjectsPrefetchTriggered(t)||this.metadataCache.prefetchAllObjects(t,async x=>F(this.context,x,!0,t,this.connectionManager))}let o=`%${e.replace(/'/g,"''").toUpperCase()}%`,h=`
            SELECT * FROM (
                SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                       COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                FROM _V_OBJECT_DATA 
                WHERE UPPER(OBJNAME) LIKE '${o}'
                UNION ALL
                SELECT 1 AS PRIORITY, OBJNAME AS NAME, SCHEMA, DBNAME AS DATABASE, OBJTYPE AS TYPE, '' AS PARENT, 
                       COALESCE(DESCRIPTION, '') AS DESCRIPTION, 'DESC' AS MATCH_TYPE
                FROM _V_OBJECT_DATA 
                WHERE UPPER(DESCRIPTION) LIKE '${o}' AND UPPER(OBJNAME) NOT LIKE '${o}'
                UNION ALL
                SELECT 2 AS PRIORITY, C.ATTNAME AS NAME, O.SCHEMA, O.DBNAME AS DATABASE, 'COLUMN' AS TYPE, O.OBJNAME AS PARENT,
                       COALESCE(C.DESCRIPTION, '') AS DESCRIPTION, 'NAME' AS MATCH_TYPE
                FROM _V_RELATION_COLUMN C
                JOIN _V_OBJECT_DATA O ON C.OBJID = O.OBJID
                WHERE UPPER(C.ATTNAME) LIKE '${o}'
                UNION ALL
                SELECT 4 AS PRIORITY, V.VIEWNAME AS NAME, V.SCHEMA, V.DATABASE, 'VIEW' AS TYPE, '' AS PARENT, 
                       'Found in view definition' AS DESCRIPTION, 'DEFINITION' AS MATCH_TYPE
                FROM _V_VIEW V
                WHERE UPPER(V.DEFINITION) LIKE '${o}'
                UNION ALL
                SELECT 4 AS PRIORITY, P.PROCEDURE AS NAME, P.SCHEMA, P.DATABASE, 'PROCEDURE' AS TYPE, '' AS PARENT, 
                       'Found in procedure source' AS DESCRIPTION, 'SOURCE' AS MATCH_TYPE
                FROM _V_PROCEDURE P
                WHERE UPPER(P.PROCEDURESOURCE) LIKE '${o}'
            ) AS R
            ORDER BY PRIORITY, NAME
            LIMIT 100
        `;try{let c=await F(this.context,h,!0,t,this.connectionManager);if(n!==this.currentSearchId){a.dispose();return}if(c){let x=JSON.parse(c),p=[];x.forEach(m=>{let g=`${m.NAME.toUpperCase().trim()}|${m.TYPE.toUpperCase().trim()}|${(m.PARENT||"").toUpperCase().trim()}`;r.has(g)||(p.push({NAME:m.NAME,SCHEMA:m.SCHEMA,DATABASE:m.DATABASE,TYPE:m.TYPE,PARENT:m.PARENT,DESCRIPTION:m.DESCRIPTION,MATCH_TYPE:m.MATCH_TYPE,connectionName:t}),r.add(g))}),p.length>0&&this._view&&this._view.webview.postMessage({type:"results",data:p,append:!0})}}catch(c){console.error("Search error:",c),this._view&&n===this.currentSearchId&&this._view.webview.postMessage({type:"error",message:c.message})}finally{a.dispose()}}_getHtmlForWebview(e){return`<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Schema Search</title>
        <style>
            body { 
                font-family: var(--vscode-font-family); 
                padding: 0; 
                margin: 0;
                color: var(--vscode-foreground); 
                display: flex;
                flex-direction: column;
                height: 100vh;
                overflow: hidden;
            }
            .search-box { 
                display: flex; 
                gap: 5px; 
                padding: 10px;
                flex-shrink: 0;
                border-bottom: 1px solid var(--vscode-panel-border);
            }
            input { flex-grow: 1; padding: 5px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); }
            button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                background-color: var(--vscode-button-secondaryBackground);
                color: var(--vscode-button-secondaryForeground);
                border: 1px solid var(--vscode-contrastBorder, transparent);
                padding: 4px 10px;
                cursor: pointer;
                border-radius: 2px;
                font-family: var(--vscode-font-family);
                font-size: 12px;
                line-height: 18px;
            }
            button:hover { background-color: var(--vscode-button-secondaryHoverBackground); }
            button.primary { background-color: var(--vscode-button-background); color: var(--vscode-button-foreground); }
            button.primary:hover { background-color: var(--vscode-button-hoverBackground); }
            #status { padding: 5px 10px; flex-shrink: 0; }
            .results { 
                list-style: none; 
                padding: 0; 
                margin: 0; 
                flex-grow: 1; 
                overflow-y: auto; 
            }
            .result-item { padding: 8px 10px; border-bottom: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; cursor: pointer; position: relative; }
            .result-item:hover { background: var(--vscode-list-hoverBackground); }
            .item-header { display: flex; justify-content: space-between; font-weight: bold; }
            .item-details { font-size: 0.9em; opacity: 0.8; display: flex; gap: 10px; }
            .type-badge { font-size: 0.8em; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); padding: 2px 5px; border-radius: 3px; }
            .tooltip { position: absolute; background: var(--vscode-editorHoverWidget-background); color: var(--vscode-editorHoverWidget-foreground); border: 1px solid var(--vscode-editorHoverWidget-border); padding: 8px; border-radius: 4px; font-size: 0.9em; max-width: 300px; word-wrap: break-word; z-index: 1000; opacity: 0; visibility: hidden; transition: opacity 0.2s, visibility 0.2s; pointer-events: none; }
            .result-item:hover .tooltip { opacity: 1; visibility: visible; }
            .tooltip.top { bottom: 100%; left: 0; margin-bottom: 5px; }
            .tooltip.bottom { top: 100%; left: 0; margin-top: 5px; }
            .cache-badge { background-color: var(--vscode-charts-green); color: white; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; margin-left: 5px; }
            .spinner {
                border: 2px solid transparent;
                border-top: 2px solid var(--vscode-progressBar-background);
                border-radius: 50%;
                width: 14px;
                height: 14px;
                animation: spin 1s linear infinite;
                display: inline-block;
                vertical-align: middle;
                margin-right: 8px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="search-box">
            <input type="text" id="searchInput" placeholder="Search tables, columns, view definitions, procedure source..." />
            <button id="searchBtn" class="primary">Search</button>
        </div>
        <div id="status"></div>
        <ul class="results" id="resultsList"></ul>

        <script>
            try {
            const vscode = acquireVsCodeApi();
            const searchInput = document.getElementById('searchInput');
            const searchBtn = document.getElementById('searchBtn');
            const resultsList = document.getElementById('resultsList');
            const status = document.getElementById('status');

            searchBtn.addEventListener('click', () => {
                const term = searchInput.value;
                if (term) {
                    status.innerHTML = '<span class="spinner"></span> Searching...';
                    vscode.postMessage({ type: 'search', value: term });
                }
            });

            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    searchBtn.click();
                }
            });

            window.addEventListener('message', event => {
                const message = event.data;
                switch (message.type) {
                    case 'results':
                        status.textContent = '';
                        renderResults(message.data, message.append);
                        break;
                    case 'error':
                        status.textContent = 'Error: ' + message.message;
                        break;
                }
            });

            function renderResults(data, append) {
                if (!append) {
                    resultsList.innerHTML = '';
                }

                if (!data || data.length === 0) {
                    if (!append && resultsList.children.length === 0) {
                        status.textContent = 'No results found.';
                    }
                    return;
                }

                data.forEach(item => {
                    const li = document.createElement('li');
                    li.className = 'result-item';

                    const parentInfo = item.PARENT ? \`Parent: \${item.PARENT}\` : '';
                    const schemaInfo = item.SCHEMA ? \`Schema: \${item.SCHEMA}\` : '';
                    const databaseInfo = item.DATABASE ? \`Database: \${item.DATABASE}\` : '';
                    const description = item.DESCRIPTION && item.DESCRIPTION.trim() ? item.DESCRIPTION : '';
                    
                    // Add match type indicator
                    const matchTypeInfo = item.MATCH_TYPE === 'DEFINITION' ? 'Match in view definition' :
                                        item.MATCH_TYPE === 'SOURCE' ? 'Match in procedure source' :
                                        item.MATCH_TYPE === 'NAME' ? 'Match in name' : '';
                    
                    li.innerHTML = \`
                        <div class="item-header">
                            <span>\${item.NAME}</span>
                            <span class="type-badge">\${item.TYPE}</span>
                        </div>
                        <div class="item-details">
                            <span>\${databaseInfo}</span>
                            <span>\${schemaInfo}</span>
                            <span>\${parentInfo}</span>
                            \${matchTypeInfo ? \`<span style="font-style: italic; color: var(--vscode-descriptionForeground);">\${matchTypeInfo}</span>\` : ''}
                        </div>
                        \${description ? \`<div class="tooltip bottom">\${description}</div>\` : ''}
                    \`;
                    
                    // Add double-click handler to navigate to schema tree
                    li.addEventListener('dblclick', () => {
                        vscode.postMessage({ 
                            type: 'navigate', 
                            name: item.NAME,
                            schema: item.SCHEMA,
                            database: item.DATABASE,
                            objType: item.TYPE,
                            parent: item.PARENT,
                            connectionName: item.connectionName // Pass back connection name
                        });
                    });
                    
                    resultsList.appendChild(li);
                });
            }
            } catch (e) {
                document.body.innerHTML = '<pre style="color:red;">Error loading Schema Search: ' + e.message + '\\n' + e.stack + '</pre>';
            }
        </script>
    </body>
    </html>`}};var re=class{static splitStatements(e){let t=[],s="",n=!1,a=!1,r=!1,i=!1,o=0;for(;o<e.length;){let h=e[o],c=o+1<e.length?e[o+1]:"";if(r)h===`
`&&(r=!1);else if(i){if(h==="*"&&c==="/"){i=!1,s+=h+c,o++,o++;continue}}else if(n)h==="'"&&e[o-1]!=="\\"&&(n=!1);else if(a)h==='"'&&e[o-1]!=="\\"&&(a=!1);else if(h==="-"&&c==="-")r=!0;else if(h==="/"&&c==="*")i=!0;else if(h==="'")n=!0;else if(h==='"')a=!0;else if(h===";"){s.trim()&&t.push(s.trim()),s="",o++;continue}s+=h,o++}return s.trim()&&t.push(s.trim()),t}static getStatementAtPosition(e,t){let s=0,n=e.length,a=!1,r=!1,i=!1,o=!1,h=-1;for(let x=0;x<t;x++){let p=e[x],m=x+1<e.length?e[x+1]:"";i?p===`
`&&(i=!1):o?p==="*"&&m==="/"&&(o=!1,x++):a?p==="'"&&e[x-1]!=="\\"&&(a=!1):r?p==='"'&&e[x-1]!=="\\"&&(r=!1):p==="-"&&m==="-"?i=!0:p==="/"&&m==="*"?o=!0:p==="'"?a=!0:p==='"'?r=!0:p===";"&&(h=x)}s=h+1,a=!1,r=!1,i=!1,o=!1;for(let x=s;x<e.length;x++){let p=e[x],m=x+1<e.length?e[x+1]:"";if(i)p===`
`&&(i=!1);else if(o)p==="*"&&m==="/"&&(o=!1,x++);else if(a)p==="'"&&e[x-1]!=="\\"&&(a=!1);else if(r)p==='"'&&e[x-1]!=="\\"&&(r=!1);else if(p==="-"&&m==="-")i=!0;else if(p==="/"&&m==="*")o=!0;else if(p==="'")a=!0;else if(p==='"')r=!0;else if(p===";"){n=x;break}}let c=e.substring(s,n).trim();return c?{sql:c,start:s,end:n}:null}static getObjectAtPosition(e,t){let s=h=>/[a-zA-Z0-9_."]/i.test(h),n=t;for(;n>0&&s(e[n-1]);)n--;let a=t;for(;a<e.length&&s(e[a]);)a++;let r=e.substring(n,a);if(!r)return null;let i=h=>h?h.replace(/"/g,""):void 0;if(r.includes("..")){let h=r.split("..");if(h.length===2)return{database:i(h[0]),name:i(h[1])}}let o=r.split(".");return o.length===1?{name:i(o[0])}:o.length===2?{schema:i(o[0]),name:i(o[1])}:o.length===3?{database:i(o[0]),schema:i(o[1]),name:i(o[2])}:null}};var we=U(require("vscode"));var Fe=class{provideDocumentLinks(e,t){let s=[],n=e.getText(),a=/[a-zA-Z0-9_"]+(\.[a-zA-Z0-9_"]*)+/g,r;for(;(r=a.exec(n))!==null;){let i=e.positionAt(r.index),o=e.positionAt(r.index+r[0].length),h=new we.Range(i,o),c=re.getObjectAtPosition(n,r.index+Math.floor(r[0].length/2));if(c){if(r[0].split(".").length===2&&!c.database&&this.isLikelyAliasReference(n,r.index))continue;let m={name:c.name,schema:c.schema,database:c.database},g=we.Uri.parse(`command:netezza.revealInSchema?${encodeURIComponent(JSON.stringify(m))}`),y=new we.DocumentLink(h,g);y.tooltip=`Reveal ${c.name} in Schema`,s.push(y)}}return s}isLikelyAliasReference(e,t){let n=e.substring(Math.max(0,t-200),t).replace(/--[^\n]*/g,"").replace(/\/\*[\s\S]*?\*\//g,"").toUpperCase();return/(?:FROM|JOIN)\s+[a-zA-Z0-9_"]*$/i.test(n)?!1:!!n.match(/\b(SELECT|WHERE|ON|HAVING|ORDER\s+BY|GROUP\s+BY|AND|OR|SET|VALUES)\b(?!.*\b(?:FROM|JOIN)\b)/)}};var ke=U(require("vscode")),ze=class{provideFoldingRanges(e,t,s){let n=[],a=[],r=/^\s*--\s*REGION\b/i,i=/^\s*--\s*ENDREGION\b/i;for(let o=0;o<e.lineCount;o++){let c=e.lineAt(o).text;if(r.test(c))a.push(o);else if(i.test(c)&&a.length>0){let x=a.pop();n.push(new ke.FoldingRange(x,o,ke.FoldingRangeKind.Region))}}return n}};var X=U(require("vscode"));_e();var Re=class{constructor(e,t){this._extensionUri=e;this._context=t}static{this.viewType="netezza.queryHistory"}resolveWebviewView(e,t,s){this._view=e,e.webview.options={enableScripts:!0,localResourceRoots:[this._extensionUri]},e.webview.html=this._getHtmlForWebview(e.webview),this.sendHistoryToWebview(),e.webview.onDidReceiveMessage(async n=>{switch(n.type){case"refresh":this.refresh();break;case"clearAll":await this.clearAllHistory();break;case"deleteEntry":await this.deleteEntry(n.id,n.query);break;case"copyQuery":await X.env.clipboard.writeText(n.query),X.window.showInformationMessage("Query copied to clipboard");break;case"executeQuery":await this.executeQuery(n.query);break;case"getHistory":await this.sendHistoryToWebview();break;case"toggleFavorite":await this.toggleFavorite(n.id);break;case"updateEntry":await this.updateEntry(n.id,n.tags,n.description);break;case"requestEdit":await this.requestEdit(n.id);break;case"requestTagFilter":await this.requestTagFilter(n.tags);break;case"showFavoritesOnly":await this.sendFavoritesToWebview();break;case"filterByTag":await this.sendFilteredByTagToWebview(n.tag);break}})}refresh(){this._view&&this.sendHistoryToWebview()}async sendHistoryToWebview(){if(!this._view)return;let e=new G(this._context),t=await e.getHistory(),s=await e.getStats();console.log("QueryHistoryView: sending history to webview, entries=",t.length),this._view.webview.postMessage({type:"historyData",history:t,stats:s})}async clearAllHistory(){await X.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await new G(this._context).clearHistory(),this.refresh(),X.window.showInformationMessage("Query history cleared"))}async deleteEntry(e,t){let s=t?`: ${t.substring(0,50)}${t.length>50?"...":""}`:"";await X.window.showWarningMessage(`Are you sure you want to delete this query${s}?`,{modal:!0},"Delete")==="Delete"&&(await new G(this._context).deleteEntry(e),this.refresh())}async executeQuery(e){let t=await X.workspace.openTextDocument({content:e,language:"sql"});await X.window.showTextDocument(t)}async toggleFavorite(e){await new G(this._context).toggleFavorite(e),this.refresh()}async updateEntry(e,t,s){await new G(this._context).updateEntry(e,t,s),this.refresh(),X.window.showInformationMessage("Entry updated successfully")}async requestEdit(e){let n=(await new G(this._context).getHistory()).find(i=>i.id===e);if(!n){X.window.showErrorMessage("Entry not found");return}let a=await X.window.showInputBox({prompt:"Enter tags (comma separated)",value:n.tags||"",placeHolder:"tag1, tag2, tag3"});if(a===void 0)return;let r=await X.window.showInputBox({prompt:"Enter description",value:n.description||"",placeHolder:"Description for this query"});r!==void 0&&await this.updateEntry(e,a,r)}async requestTagFilter(e){if(e.length===1)await this.sendFilteredByTagToWebview(e[0]);else if(e.length>1){let t=await X.window.showQuickPick(e,{placeHolder:"Filter by which tag?"});t&&await this.sendFilteredByTagToWebview(t)}}async sendFavoritesToWebview(){if(!this._view)return;let e=new G(this._context),t=await e.getFavorites(),s=await e.getStats();this._view.webview.postMessage({type:"historyData",history:t,stats:s,filter:"favorites"})}async sendFilteredByTagToWebview(e){if(!this._view)return;let t=new G(this._context),s=await t.getByTag(e),n=await t.getStats();this._view.webview.postMessage({type:"historyData",history:s,stats:n,filter:`tag: ${e}`})}_getHtmlForWebview(e){let t=mn(),s=e.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","queryHistory.css")),n=e.asWebviewUri(X.Uri.joinPath(this._extensionUri,"media","queryHistory.js"));return`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${e.cspSource}; script-src ${e.cspSource};">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query History</title>
    <link href="${s}" rel="stylesheet">
</head>
<body>
    <div class="toolbar">
        <div class="toolbar-top">
            <input type="search" id="searchInput" placeholder="Search queries..." />
            <span class="stats" id="stats">Loading...</span>
        </div>
        <div class="toolbar-buttons">
            <button class="secondary" id="showAllBtn">\u{1F4DC} All</button>
            <button class="secondary" id="showFavoritesBtn">\u2B50 Favorites</button>
            <button class="secondary" id="refreshBtn">\u21BB Refresh</button>
            <button class="secondary" id="clearAllBtn">\u{1F5D1}\uFE0F Clear All</button>
        </div>
    </div>
    <div class="history-container" id="historyContainer">
        <div class="empty-state">
            <div class="empty-state-icon">\u{1F4DC}</div>
            <div>No query history yet</div>
        </div>
    </div>

    <script src="${n}"></script>
</body>
</html>`}};function mn(){let u="",e="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";for(let t=0;t<32;t++)u+=e.charAt(Math.floor(Math.random()*e.length));return u}var Ue=class{constructor(e){this.context=e;this.dbCache=new Map;this.schemaCache=new Map;this.tableCache=new Map;this.columnCache=new Map;this.tableIdMap=new Map;this.CACHE_TTL=14400*1e3;this.savePending=!1;this.pendingCacheTypes=new Set;this.columnPrefetchInProgress=new Set;this.allObjectsPrefetchTriggeredSet=new Set;this.connectionPrefetchTriggered=new Set;this.connectionPrefetchInProgress=new Set;this.loadCacheFromWorkspaceState()}loadCacheFromWorkspaceState(){try{let e=Date.now(),t=this.context.workspaceState.get("sqlCompletion.dbCache");if(t&&!(t.data&&Array.isArray(t.data)))for(let[i,o]of Object.entries(t))e-o.timestamp<this.CACHE_TTL&&this.dbCache.set(i,o);let s=this.context.workspaceState.get("sqlCompletion.schemaCache");if(s)for(let[i,o]of Object.entries(s))e-o.timestamp<this.CACHE_TTL&&this.schemaCache.set(i,o);let n=this.context.workspaceState.get("sqlCompletion.tableCache");if(n)for(let[i,o]of Object.entries(n))e-o.timestamp<this.CACHE_TTL&&this.tableCache.set(i,o);let a=this.context.workspaceState.get("sqlCompletion.tableIdMap");if(a)for(let[i,o]of Object.entries(a))(this.tableCache.has(i)||e-o.timestamp<this.CACHE_TTL)&&this.tableIdMap.set(i,{data:new Map(Object.entries(o.data)),timestamp:o.timestamp});let r=this.context.workspaceState.get("sqlCompletion.columnCache");if(r)for(let[i,o]of Object.entries(r))e-o.timestamp<this.CACHE_TTL&&this.columnCache.set(i,o)}catch(e){console.error("[MetadataCache] Error loading cache from workspace state:",e)}}scheduleSave(e){this.pendingCacheTypes.add(e),this.savePending||(this.savePending=!0,this.saveTimeoutId=setTimeout(()=>this.flushSave(),1e3))}async flushSave(){this.savePending=!1;let e=new Set(this.pendingCacheTypes);this.pendingCacheTypes.clear();try{if(e.has("db")&&this.dbCache.size>0){let t={};this.dbCache.forEach((s,n)=>{t[n]=s}),await this.context.workspaceState.update("sqlCompletion.dbCache",t)}if(e.has("schema")&&this.schemaCache.size>0){let t={};this.schemaCache.forEach((s,n)=>{t[n]=s}),await this.context.workspaceState.update("sqlCompletion.schemaCache",t)}if(e.has("table")&&this.tableCache.size>0){let t={};this.tableCache.forEach((n,a)=>{t[a]=n}),await this.context.workspaceState.update("sqlCompletion.tableCache",t);let s={};this.tableIdMap.forEach((n,a)=>{let r={};n.data.forEach((i,o)=>{r[o]=i}),s[a]={data:r,timestamp:n.timestamp}}),await this.context.workspaceState.update("sqlCompletion.tableIdMap",s)}if(e.has("column")&&this.columnCache.size>0){let t={};this.columnCache.forEach((s,n)=>{t[n]=s}),await this.context.workspaceState.update("sqlCompletion.columnCache",t)}}catch(t){console.error("[MetadataCache] Error saving cache to workspace state:",t)}}async clearCache(){this.saveTimeoutId&&(clearTimeout(this.saveTimeoutId),this.saveTimeoutId=void 0),this.savePending=!1,this.pendingCacheTypes.clear(),this.dbCache.clear(),this.schemaCache.clear(),this.tableCache.clear(),this.columnCache.clear(),this.tableIdMap.clear(),await this.context.workspaceState.update("sqlCompletion.dbCache",void 0),await this.context.workspaceState.update("sqlCompletion.schemaCache",void 0),await this.context.workspaceState.update("sqlCompletion.tableCache",void 0),await this.context.workspaceState.update("sqlCompletion.columnCache",void 0),await this.context.workspaceState.update("sqlCompletion.tableIdMap",void 0)}getDatabases(e){return this.dbCache.get(e)?.data}setDatabases(e,t){this.dbCache.set(e,{data:t,timestamp:Date.now()}),this.scheduleSave("db")}getSchemas(e,t){let s=`${e}|${t}`;return this.schemaCache.get(s)?.data}setSchemas(e,t,s){let n=`${e}|${t}`;this.schemaCache.set(n,{data:s,timestamp:Date.now()}),this.scheduleSave("schema")}getTables(e,t){let s=`${e}|${t}`;return this.tableCache.get(s)?.data}setTables(e,t,s,n){let a=Date.now(),r=`${e}|${t}`;this.tableCache.set(r,{data:s,timestamp:a}),this.tableIdMap.set(r,{data:n,timestamp:a}),this.scheduleSave("table")}getColumns(e,t){let s=`${e}|${t}`;return this.columnCache.get(s)?.data}setColumns(e,t,s){let n=`${e}|${t}`;this.columnCache.set(n,{data:s,timestamp:Date.now()}),this.scheduleSave("column")}findTableId(e,t){let s=`${e}|`;for(let[n,a]of this.tableIdMap)if(n.startsWith(s)){let r=a.data.get(t);if(r!==void 0)return r}}search(e,t){let s=[],n=e.toLowerCase(),a=r=>t?r.startsWith(`${t}|`):!0;for(let[r,i]of this.tableCache){if(!a(r))continue;let o=r.split("|");if(o.length<2)continue;let c=o[1].split("."),x=c[0],p=c.length>1?c[1]:void 0;for(let m of i.data){let g=typeof m.label=="string"?m.label:m.label.label;g&&g.toLowerCase().includes(n)&&s.push({name:g,type:"TABLE",database:x,schema:p||(m.detail&&m.detail.includes("(")?m.detail.match(/\((.*?)\)/)?.[1]:void 0)})}}for(let[r,i]of this.columnCache){if(!a(r))continue;let o=r.split("|");if(o.length<2)continue;let c=o[1].split("."),x=c[0],p=c[1],m=c[2];for(let g of i.data){let y=typeof g.label=="string"?g.label:g.label.label;y&&y.toLowerCase().includes(n)&&s.push({name:y,type:"COLUMN",database:x,schema:p,parent:m})}}return s}async prefetchColumnsForSchema(e,t,s,n){let a=s?`${t}.${s}`:`${t}..`,r=`${e}|${a}`;if(this.columnPrefetchInProgress.has(r))return;let i=this.getTables(e,a);if(!(!i||i.length===0)){this.columnPrefetchInProgress.add(r);try{let o=[];for(let c of i){let x=typeof c.label=="string"?c.label:c.label?.label;if(!x)continue;let p=`${t}.${s||""}.${x}`;this.getColumns(e,p)||o.push(x)}if(o.length===0)return;let h=10;for(let c=0;c<o.length;c+=h){let p=o.slice(c,c+h).map(w=>`'${w}'`).join(","),m=`${t}..`,g=s?`AND UPPER(O.SCHEMA) = UPPER('${s}')`:"",y=`
                    SELECT O.OBJNAME AS TABLENAME, C.ATTNAME, C.FORMAT_TYPE, C.ATTNUM
                    FROM ${m}_V_RELATION_COLUMN C
                    JOIN ${m}_V_OBJECT_DATA O ON C.OBJID = O.OBJID
                    WHERE UPPER(O.OBJNAME) IN (${p.toUpperCase()}) 
                    ${g}
                    AND UPPER(O.DBNAME) = UPPER('${t}')
                    ORDER BY O.OBJNAME, C.ATTNUM
                `;try{let w=await n(y);if(w){let A=JSON.parse(w),M=new Map;for(let D of A){let L=D.TABLENAME;M.has(L)||M.set(L,[]),M.get(L).push({label:D.ATTNAME,kind:5,detail:D.FORMAT_TYPE})}for(let[D,L]of M){let z=`${t}.${s||""}.${D}`;this.setColumns(e,z,L)}}}catch(w){console.error("[MetadataCache] Error fetching batch columns:",w)}}}finally{this.columnPrefetchInProgress.delete(r)}}}async prefetchAllObjects(e,t){let s=`ALL_OBJECTS|${e}`;if(!this.allObjectsPrefetchTriggeredSet.has(s)){this.allObjectsPrefetchTriggeredSet.add(s),console.log(`[MetadataCache] Starting background prefetch of all objects for search (Connection: ${e})`);try{let a=await t(`
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME 
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE = 'TABLE' 
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `);if(!a)return;let r=JSON.parse(a),i=new Map;for(let o of r){let h=o.SCHEMA?`${o.DBNAME}.${o.SCHEMA}`:`${o.DBNAME}..`;i.has(h)||i.set(h,{tables:[],idMap:new Map});let c=i.get(h);c.tables.push({label:o.OBJNAME,kind:7,detail:o.SCHEMA?"Table":`Table (${o.SCHEMA})`,sortText:o.OBJNAME});let x=o.SCHEMA?`${o.DBNAME}.${o.SCHEMA}.${o.OBJNAME}`:`${o.DBNAME}..${o.OBJNAME}`;c.idMap.set(x,o.OBJID)}for(let[o,h]of i)this.setTables(e,o,h.tables,h.idMap);console.log(`[MetadataCache] Prefetched tables for ${i.size} schema(s) on ${e}`)}catch(n){console.error("[MetadataCache] Error in prefetchAllObjects:",n)}}}hasAllObjectsPrefetchTriggered(e){return this.allObjectsPrefetchTriggeredSet.has(`ALL_OBJECTS|${e}`)}hasConnectionPrefetchTriggered(e){return this.connectionPrefetchTriggered.has(e)}triggerConnectionPrefetch(e,t){this.connectionPrefetchTriggered.has(e)||this.connectionPrefetchInProgress.has(e)||(this.connectionPrefetchInProgress.add(e),console.log(`[MetadataCache] Starting eager prefetch for connection: ${e}`),this.executeConnectionPrefetch(e,t).catch(s=>console.error("[MetadataCache] Connection prefetch error:",s)).finally(()=>{this.connectionPrefetchInProgress.delete(e),this.connectionPrefetchTriggered.add(e),console.log(`[MetadataCache] Completed eager prefetch for connection: ${e}`)}))}async executeConnectionPrefetch(e,t){let s=await this.prefetchDatabases(e,t);if(!(!s||s.length===0)){for(let n of s)await this.prefetchSchemasForDb(e,n,t),await new Promise(a=>setTimeout(a,50));await this.prefetchAllTablesAndViews(e,t),await this.prefetchAllColumnsForConnection(e,t)}}async prefetchDatabases(e,t){if(this.getDatabases(e))return this.getDatabases(e)?.map(n=>typeof n.label=="string"?n.label:n.label?.label).filter(Boolean)||[];try{let n=await t("SELECT DATABASE FROM system.._v_database ORDER BY DATABASE");if(!n)return[];let a=JSON.parse(n),r=a.map(i=>({label:i.DATABASE,kind:9,detail:"Database"}));return this.setDatabases(e,r),a.map(i=>i.DATABASE)}catch(s){return console.error("[MetadataCache] prefetchDatabases error:",s),[]}}async prefetchSchemasForDb(e,t,s){if(!this.getSchemas(e,t))try{let n=`SELECT SCHEMA FROM ${t}.._V_SCHEMA ORDER BY SCHEMA`,a=await s(n);if(!a)return;let i=JSON.parse(a).filter(o=>o.SCHEMA!=null&&o.SCHEMA!=="").map(o=>({label:o.SCHEMA,kind:19,detail:`Schema in ${t}`,insertText:o.SCHEMA,sortText:o.SCHEMA,filterText:o.SCHEMA}));this.setSchemas(e,t,i)}catch(n){console.error(`[MetadataCache] prefetchSchemasForDb error for ${t}:`,n)}}async prefetchAllTablesAndViews(e,t){try{let n=await t(`
                SELECT OBJNAME, OBJID, SCHEMA, DBNAME, OBJTYPE
                FROM _V_OBJECT_DATA 
                WHERE OBJTYPE IN ('TABLE', 'VIEW')
                ORDER BY DBNAME, SCHEMA, OBJNAME
            `);if(!n)return;let a=JSON.parse(n),r=new Map;for(let i of a){let o=i.SCHEMA?`${i.DBNAME}.${i.SCHEMA}`:`${i.DBNAME}..`;r.has(o)||r.set(o,{tables:[],idMap:new Map});let h=r.get(o);h.tables.push({label:i.OBJNAME,kind:i.OBJTYPE==="VIEW"?18:7,detail:i.SCHEMA?i.OBJTYPE:`${i.OBJTYPE} (${i.SCHEMA})`,sortText:i.OBJNAME});let c=i.SCHEMA?`${i.DBNAME}.${i.SCHEMA}.${i.OBJNAME}`:`${i.DBNAME}..${i.OBJNAME}`;h.idMap.set(c,i.OBJID)}for(let[i,o]of r)this.getTables(e,i)||this.setTables(e,i,o.tables,o.idMap);console.log(`[MetadataCache] Prefetched tables/views for ${r.size} schema(s)`)}catch(s){console.error("[MetadataCache] prefetchAllTablesAndViews error:",s)}}async prefetchAllColumnsForConnection(e,t){try{let s=`${e}|`,n=[];for(let[i,o]of this.tableCache){if(!i.startsWith(s))continue;let h=i.split("|");if(h.length<2)continue;let x=h[1].split("."),p=x[0],m=x.length>1?x[1]:"";for(let g of o.data){let y=typeof g.label=="string"?g.label:g.label.label;y&&n.push({schema:m,name:y,db:p})}}if(n.length===0)return;let a=50,r=0;for(let i=0;i<n.length;i+=a){let o=n.slice(i,i+a),h=new Map;for(let c of o)h.has(c.db)||h.set(c.db,[]),h.get(c.db).push(c);for(let[c,x]of h){let p=x.map(g=>`(UPPER(O.SCHEMA) = UPPER('${g.schema}') AND UPPER(O.OBJNAME) = UPPER('${g.name}'))`).join(" OR "),m=`
                        SELECT O.OBJNAME AS TABLENAME, O.SCHEMA, O.DBNAME, 
                               C.ATTNAME, C.FORMAT_TYPE, C.ATTNUM
                        FROM ${c}.._V_RELATION_COLUMN C
                        JOIN ${c}.._V_OBJECT_DATA O ON C.OBJID = O.OBJID
                        WHERE O.DBNAME = '${c}' 
                        AND (${p})
                        ORDER BY O.SCHEMA, O.OBJNAME, C.ATTNUM
                    `;try{let g=await t(m);if(g){let y=JSON.parse(g),w=new Map;for(let A of y){let M=`${A.DBNAME}.${A.SCHEMA||""}.${A.TABLENAME}`;w.has(M)||w.set(M,[]),w.get(M).push({label:A.ATTNAME,kind:5,detail:A.FORMAT_TYPE})}for(let[A,M]of w)this.getColumns(e,A)||(this.setColumns(e,A,M),r++)}}catch(g){console.error(`[MetadataCache] Error fetching batch columns for DB ${c}:`,g)}}await new Promise(c=>setTimeout(c,10))}console.log(`[MetadataCache] Prefetched columns for ${r} tables/views (Batched)`)}catch(s){console.error("[MetadataCache] prefetchAllColumnsForConnection error:",s)}}};var on=U(require("path"));function nn(u,e){let t=e.getKeepConnectionOpen();u.text=t?"\u{1F517} Keep Connection ON":"\u26D3\uFE0F\u200D\u{1F4A5} Keep Connection OFF",u.tooltip=t?"Keep Connection Open: ENABLED - Click to disable":"Keep Connection Open: DISABLED - Click to enable",u.backgroundColor=t?new l.ThemeColor("statusBarItem.prominentBackground"):void 0}var sn=[{id:"mtxr.sqltools",name:"SQLTools"},{id:"ms-mssql.mssql",name:"Microsoft SQL Server"},{id:"oracle.oracledevtools",name:"Oracle Developer Tools"},{id:"cweijan.vscode-mysql-client2",name:"MySQL"},{id:"ckolkman.vscode-postgres",name:"PostgreSQL"}];async function Jn(u){let e=l.workspace.getConfiguration("netezza");if(!e.get("showConflictWarnings",!0))return;let s=[];for(let i of sn)l.extensions.getExtension(i.id)&&s.push(i.name);let a=l.extensions.all.filter(i=>{let o=i.packageJSON;if(!o||i.id==="krzysztof-d.justybaselite-netezza"||sn.some(x=>x.id===i.id))return!1;let h=o.activationEvents?.some(x=>x.includes("onLanguage:sql")||x.includes("onLanguage:mssql")),c=o.contributes?.languages?.some(x=>x.id==="sql"||x.extensions?.includes(".sql"));return h||c}).map(i=>i.packageJSON.displayName||i.id),r=[...s,...a];if(r.length>0){let i=r.length===1?`Wykryto rozszerzenie SQL "${r[0]}" kt\xF3re mo\u017Ce powodowa\u0107 konflikty (np. zduplikowane skr\xF3ty klawiszowe F5, Ctrl+Enter).`:`Wykryto rozszerzenia SQL kt\xF3re mog\u0105 powodowa\u0107 konflikty: ${r.join(", ")}. Niekt\xF3re funkcje (np. F5, Ctrl+Enter) mog\u0105 by\u0107 zduplikowane.`;await l.window.showWarningMessage(i,"OK","Nie pokazuj ponownie")==="Nie pokazuj ponownie"&&await e.update("showConflictWarnings",!1,l.ConfigurationTarget.Global)}}function Xn(u){console.log("Netezza extension: Activating..."),Jn(u),u.subscriptions.push({dispose:()=>{e.closeAllPersistentConnections()}});let e=new he(u),t=new Ue(u),s=new Te(u,e,t),n=new Se(u.extensionUri),a=l.window.createStatusBarItem(l.StatusBarAlignment.Left,100);a.command="netezza.selectConnectionForTab",a.tooltip="Click to select connection for this SQL tab",u.subscriptions.push(a);let r=()=>{let d=l.window.activeTextEditor;if(d&&d.document.languageId==="sql"){let f=d.document.uri.toString(),E=e.getConnectionForExecution(f);E?(a.text=`$(database) ${E}`,a.show()):(a.text="$(database) Select Connection",a.show())}else a.hide()};r(),e.onDidChangeActiveConnection(d=>{r(),d&&!t.hasConnectionPrefetchTriggered(d)&&t.triggerConnectionPrefetch(d,f=>F(u,f,!0,d,e))}),e.onDidChangeConnections(r),e.onDidChangeDocumentConnection(d=>{r();let f=e.getDocumentConnection(d);f&&!t.hasConnectionPrefetchTriggered(f)&&t.triggerConnectionPrefetch(f,E=>F(u,E,!0,f,e))}),l.window.onDidChangeActiveTextEditor(r);let i=l.window.createStatusBarItem(l.StatusBarAlignment.Right,100);i.command="netezza.toggleKeepConnectionOpen",nn(i,e),i.show(),u.subscriptions.push(i),console.log("Netezza extension: Registering SchemaSearchProvider...");let o=new Ie(u.extensionUri,u,t,e);console.log("Netezza extension: Registering QueryHistoryView...");let h=new Re(u.extensionUri,u),c=l.window.createTreeView("netezza.schema",{treeDataProvider:s,showCollapseAll:!0});u.subscriptions.push(l.window.registerWebviewViewProvider(Se.viewType,n),l.window.registerWebviewViewProvider(Ie.viewType,o),l.window.registerWebviewViewProvider(Re.viewType,h));let x=/(^|\s)(?:[A-Za-z]:\\|\\|\/)?[\w.\-\\\/]+\.py\b|(^|\s)python(?:\.exe)?\s+[^\n]*\.py\b/i;function p(d){return d&&(d.includes(" ")?`"${d.replace(/"/g,'\\"')}"`:d)}function m(d,f,E){let C=/[ \\/]/.test(d)?`& ${p(d)}`:d,T=p(f),S=E.map(b=>p(b)).join(" ");return`${C} ${T}${S?" "+S:""}`.trim()}class g{constructor(){this._onDidChange=new l.EventEmitter;this.onDidChangeCodeLenses=this._onDidChange.event}provideCodeLenses(f){let E=[];for(let v=0;v<f.lineCount;v++){let C=f.lineAt(v);if(x.test(C.text)){let T=C.range,S={title:"Run as script",command:"netezza.runScriptFromLens",arguments:[f.uri,T]};E.push(new l.CodeLens(T,S))}}return E}refresh(){this._onDidChange.fire()}}let y=new g;u.subscriptions.push(l.languages.registerCodeLensProvider({scheme:"file"},y));let w=l.window.createTextEditorDecorationType({backgroundColor:new l.ThemeColor("editor.rangeHighlightBackground"),borderRadius:"3px"});function A(d){let f=d||l.window.activeTextEditor;if(!f)return;let E=f.document,v=[];for(let C=0;C<E.lineCount;C++){let T=E.lineAt(C);x.test(T.text)&&v.push({range:T.range,hoverMessage:"Python script invocation"})}f.setDecorations(w,v)}u.subscriptions.push(l.window.onDidChangeActiveTextEditor(()=>A()),l.workspace.onDidChangeTextDocument(d=>{l.window.activeTextEditor&&d.document===l.window.activeTextEditor.document&&A(l.window.activeTextEditor)})),u.subscriptions.push(l.commands.registerCommand("netezza.runScriptFromLens",async(d,f)=>{try{let E=await l.workspace.openTextDocument(d),v=E.getText(f).trim()||E.lineAt(f.start.line).text.trim();if(!v){l.window.showWarningMessage("No script command found");return}let C=v.split(/\s+/),T=C[0]||"",S=/python(\\.exe)?$/i.test(T)&&C.length>=2&&C[1].toLowerCase().endsWith(".py"),b=T.toLowerCase().endsWith(".py"),N=l.workspace.getConfiguration("netezza").get("pythonPath")||"python",R="";if(S){let O=C[0],W=C[1],K=C.slice(2);R=m(O,W,K)}else if(b){let O=T,W=C.slice(1);R=m(N,O,W)}else R=m(N,"",C);let $=l.window.createTerminal({name:"Netezza: Script"});$.show(!0),$.sendText(R,!0),l.window.showInformationMessage(`Running script: ${R}`)}catch(E){l.window.showErrorMessage(`Error running script: ${E.message}`)}})),A(l.window.activeTextEditor);let M=l.window.createTextEditorDecorationType({backgroundColor:"rgba(5, 115, 201, 0.10)",isWholeLine:!1,rangeBehavior:l.DecorationRangeBehavior.ClosedClosed});function D(d){if(!l.workspace.getConfiguration("netezza").get("highlightActiveStatement",!0)||!d||d.document.languageId!=="sql"&&d.document.languageId!=="mssql"){d&&d.setDecorations(M,[]);return}try{let v=d.document,C=d.selection.active,T=v.offsetAt(C),S=v.getText(),b=re.getStatementAtPosition(S,T);if(b){let I=v.positionAt(b.start),N=v.positionAt(b.end),R=new l.Range(I,N);d.setDecorations(M,[R])}else d.setDecorations(M,[])}catch(v){console.error("Error updating SQL highlight:",v)}}u.subscriptions.push(l.window.onDidChangeTextEditorSelection(d=>{D(d.textEditor)}),l.window.onDidChangeActiveTextEditor(d=>{D(d)}),l.workspace.onDidChangeConfiguration(d=>{d.affectsConfiguration("netezza.highlightActiveStatement")&&D(l.window.activeTextEditor)})),D(l.window.activeTextEditor),u.subscriptions.push(l.window.onDidChangeActiveTextEditor(d=>{if(d&&d.document){let f=d.document.uri.toString();n.setActiveSource(f)}})),u.subscriptions.push(l.commands.registerCommand("netezza.toggleKeepConnectionOpen",()=>{let d=e.getKeepConnectionOpen();e.setKeepConnectionOpen(!d),nn(i,e);let f=!d;l.window.showInformationMessage(f?"Keep connection open: ENABLED - connection will remain open after queries":"Keep connection open: DISABLED - connection will be closed after each query")}),l.commands.registerCommand("netezza.dropCurrentSession",async()=>{let d=Ke(),f=Ge();if(!d){l.window.showWarningMessage("No active session to drop. Run a query first.");return}if(await l.window.showWarningMessage(`Are you sure you want to drop session ${d}? This will kill the current running query.`,{modal:!0},"Yes, drop session","Cancel")==="Yes, drop session")try{let v=await e.getConnectionString(f);if(!v){l.window.showErrorMessage("No connection available to execute DROP SESSION");return}let T=await require("odbc").connect({connectionString:v,fetchArray:!0});try{await T.query(`DROP SESSION ${d}`),l.window.showInformationMessage(`Session ${d} dropped successfully.`),$e()}finally{await T.close()}}catch(v){l.window.showErrorMessage(`Failed to drop session: ${v.message}`)}}),l.commands.registerCommand("netezza.selectActiveConnection",async()=>{let d=await e.getConnections();if(d.length===0){l.window.showWarningMessage("No connections configured. Please connect first.");return}let f=await l.window.showQuickPick(d.map(E=>E.name),{placeHolder:"Select Active Connection"});f&&(await e.setActiveConnection(f),l.window.showInformationMessage(`Active connection set to: ${f}`))}),l.commands.registerCommand("netezza.selectConnectionForTab",async()=>{let d=l.window.activeTextEditor;if(!d||d.document.languageId!=="sql"){l.window.showWarningMessage("This command is only available for SQL files");return}let f=await e.getConnections();if(f.length===0){l.window.showWarningMessage("No connections configured. Please connect first.");return}let E=d.document.uri.toString(),v=e.getDocumentConnection(E)||e.getActiveConnectionName(),C=f.map(S=>({label:S.name,description:v===S.name?"$(check) Currently selected":`${S.host}:${S.port}/${S.database}`,detail:(v===S.name,void 0),name:S.name})),T=await l.window.showQuickPick(C,{placeHolder:"Select connection for this SQL tab"});T&&(e.setDocumentConnection(E,T.name),l.window.showInformationMessage(`Connection for this tab set to: ${T.name}`))}),l.commands.registerCommand("netezza.openLogin",()=>{Le.createOrShow(u.extensionUri,e)}),l.commands.registerCommand("netezza.refreshSchema",()=>{s.refresh(),l.window.showInformationMessage("Schema refreshed")}),l.commands.registerCommand("netezza.copySelectAll",d=>{if(d&&d.label&&d.dbName&&d.schema){let f=`SELECT * FROM ${d.dbName}.${d.schema}.${d.label} LIMIT 100;`;l.env.clipboard.writeText(f),l.window.showInformationMessage("Copied to clipboard")}}),l.commands.registerCommand("netezza.copyDrop",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let f=`${d.dbName}.${d.schema}.${d.label}`,E=`DROP ${d.objType} ${f};`;if(await l.window.showWarningMessage(`Are you sure you want to delete ${d.objType.toLowerCase()} "${f}"?`,{modal:!0},"Yes, delete","Cancel")==="Yes, delete")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Deleting ${d.objType.toLowerCase()} ${f}...`,cancellable:!1},async T=>{await F(u,E,!0,d.connectionName,e)}),l.window.showInformationMessage(`Deleted ${d.objType.toLowerCase()}: ${f}`),s.refresh()}catch(C){l.window.showErrorMessage(`Error during deletion: ${C.message}`)}}}),l.commands.registerCommand("netezza.copyName",d=>{if(d&&d.label&&d.dbName&&d.schema){let f=`${d.dbName}.${d.schema}.${d.label}`;l.env.clipboard.writeText(f),l.window.showInformationMessage("Copied to clipboard")}}),l.commands.registerCommand("netezza.grantPermissions",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType){let f=`${d.dbName}.${d.schema}.${d.label}`,E=await l.window.showQuickPick([{label:"SELECT",description:"Privileges to read data"},{label:"INSERT",description:"Privileges to insert data"},{label:"UPDATE",description:"Privileges to update data"},{label:"DELETE",description:"Privileges to delete data"},{label:"ALL",description:"All privileges (SELECT, INSERT, UPDATE, DELETE)"},{label:"LIST",description:"Privileges to list objects"}],{placeHolder:"Select privilege type"});if(!E)return;let v=await l.window.showInputBox({prompt:"Enter user or group name",placeHolder:"e.g. SOME_USER or GROUP_NAME",validateInput:S=>!S||S.trim().length===0?"User/group name cannot be empty":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(S.trim())?null:"Invalid user/group name"});if(!v)return;let C=`GRANT ${E.label} ON ${f} TO ${v.trim().toUpperCase()};`;if(await l.window.showInformationMessage(`Execute: ${C}`,{modal:!0},"Yes, execute","Cancel")==="Yes, execute")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Granting ${E.label} on ${f}...`,cancellable:!1},async b=>{await F(u,C,!0,d.connectionName,e)}),l.window.showInformationMessage(`Granted ${E.label} on ${f} to ${v.trim().toUpperCase()}`)}catch(S){l.window.showErrorMessage(`Error granting privileges: ${S.message}`)}}}),l.commands.registerCommand("netezza.groomTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,E=await l.window.showQuickPick([{label:"RECORDS ALL",description:"Groom all records (reclaim space from deleted rows)"},{label:"RECORDS READY",description:"Groom only ready records"},{label:"PAGES ALL",description:"Groom all pages (reorganize data pages)"},{label:"PAGES START",description:"Groom pages from start"},{label:"VERSIONS",description:"Groom versions (clean up old row versions)"}],{placeHolder:"Select GROOM mode"});if(!E)return;let v=await l.window.showQuickPick([{label:"DEFAULT",description:"Use default backupset",value:"DEFAULT"},{label:"NONE",description:"No backupset",value:"NONE"},{label:"Custom",description:"Specify custom backupset ID",value:"CUSTOM"}],{placeHolder:"Select RECLAIM BACKUPSET option"});if(!v)return;let C=v.value;if(v.value==="CUSTOM"){let b=await l.window.showInputBox({prompt:"Enter backupset ID",placeHolder:"np. 12345",validateInput:I=>!I||I.trim().length===0?"Backupset ID cannot be empty":/^\d+$/.test(I.trim())?null:"Backupset ID must be a number"});if(!b)return;C=b.trim()}let T=`GROOM TABLE ${f} ${E.label} RECLAIM BACKUPSET ${C};`;if(await l.window.showWarningMessage(`Execute GROOM on table "${f}"?

${T}

Warning: This operation may be time-consuming for large tables.`,{modal:!0},"Yes, execute","Cancel")==="Yes, execute")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}let I=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:`GROOM TABLE ${f} (${E.label})...`,cancellable:!1},async R=>{await F(u,T,!0,d.connectionName,e)});let N=((Date.now()-I)/1e3).toFixed(1);l.window.showInformationMessage(`GROOM completed successfully (${N}s): ${f}`)}catch(b){l.window.showErrorMessage(`Error during GROOM: ${b.message}`)}}}),l.commands.registerCommand("netezza.addTableComment",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,E=await l.window.showInputBox({prompt:"Enter comment for table",placeHolder:"e.g. Table contains customer data",value:d.objectDescription||""});if(E===void 0)return;let v=`COMMENT ON TABLE ${f} IS '${E.replace(/'/g,"''")}';`;try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}await F(u,v,!0,d.connectionName,e),l.window.showInformationMessage(`Comment added to table: ${f}`),s.refresh()}catch(C){l.window.showErrorMessage(`Error adding comment: ${C.message}`)}}}),l.commands.registerCommand("netezza.generateStatistics",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,E=`GENERATE EXPRESS STATISTICS ON ${f};`;if(await l.window.showInformationMessage(`Generate statistics for table "${f}"?

${E}`,{modal:!0},"Yes, generate","Cancel")==="Yes, generate")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}let T=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Generating statistics for ${f}...`,cancellable:!1},async b=>{await F(u,E,!0,d.connectionName,e)});let S=((Date.now()-T)/1e3).toFixed(1);l.window.showInformationMessage(`Statistics generated successfully (${S}s): ${f}`)}catch(C){l.window.showErrorMessage(`Error generating statistics: ${C.message}`)}}}),l.commands.registerCommand("netezza.truncateTable",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,E=`TRUNCATE TABLE ${f};`;if(await l.window.showWarningMessage(`\u26A0\uFE0F WARNING: Are you sure you want to delete ALL data from the table "${f}"?

${E}

This operation is IRREVERSIBLE!`,{modal:!0},"Yes, delete all data","Cancel")==="Yes, delete all data")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Clearing table ${f}...`,cancellable:!1},async T=>{await F(u,E,!0,d.connectionName,e)}),l.window.showInformationMessage(`Table cleared: ${f}`)}catch(C){l.window.showErrorMessage(`Error clearing table: ${C.message}`)}}}),l.commands.registerCommand("netezza.addPrimaryKey",async d=>{if(d&&d.label&&d.dbName&&d.schema&&d.objType==="TABLE"){let f=`${d.dbName}.${d.schema}.${d.label}`,E=await l.window.showInputBox({prompt:"Enter primary key constraint name",placeHolder:`e.g. PK_${d.label}`,value:`PK_${d.label}`,validateInput:b=>!b||b.trim().length===0?"Constraint name cannot be empty":/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(b.trim())?null:"Invalid constraint name"});if(!E)return;let v=await l.window.showInputBox({prompt:"Enter primary key column names (comma separated)",placeHolder:"e.g. COL1, COL2 or ID",validateInput:b=>!b||b.trim().length===0?"You must provide at least one column":null});if(!v)return;let C=v.split(",").map(b=>b.trim().toUpperCase()).join(", "),T=`ALTER TABLE ${f} ADD CONSTRAINT ${E.trim().toUpperCase()} PRIMARY KEY (${C});`;if(await l.window.showInformationMessage(`Add primary key to table "${f}"?

${T}`,{modal:!0},"Yes, add","Cancel")==="Yes, add")try{if(!await e.getConnectionString()){l.window.showErrorMessage("No database connection");return}await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Adding primary key to ${f}...`,cancellable:!1},async I=>{await F(u,T,!0,d.connectionName,e)}),l.window.showInformationMessage(`Primary key added: ${E.trim().toUpperCase()}`),s.refresh()}catch(b){l.window.showErrorMessage(`Error adding primary key: ${b.message}`)}}}),l.commands.registerCommand("netezza.createDDL",async d=>{try{if(!d||!d.label||!d.dbName||!d.schema||!d.objType){l.window.showErrorMessage("Invalid object selected for DDL generation");return}let f=await e.getConnectionString();if(!f){l.window.showErrorMessage("Connection not configured. Please connect via Netezza: Connect...");return}await l.window.withProgress({location:l.ProgressLocation.Notification,title:`Generating DDL for ${d.objType} ${d.label}...`,cancellable:!1},async()=>{let{generateDDL:E}=await Promise.resolve().then(()=>(Mt(),Nt)),v=await E(f,d.dbName,d.schema,d.label,d.objType);if(v.success&&v.ddlCode){let C=await l.window.showQuickPick([{label:"Open in Editor",description:"Open DDL code in a new editor",value:"editor"},{label:"Copy to Clipboard",description:"Copy DDL code to clipboard",value:"clipboard"}],{placeHolder:"How would you like to access the DDL code?"});if(C)if(C.value==="editor"){let T=await l.workspace.openTextDocument({content:v.ddlCode,language:"sql"});await l.window.showTextDocument(T),l.window.showInformationMessage(`DDL code generated for ${d.objType} ${d.label}`)}else C.value==="clipboard"&&(await l.env.clipboard.writeText(v.ddlCode),l.window.showInformationMessage("DDL code copied to clipboard"))}else throw new Error(v.error||"DDL generation failed")})}catch(f){l.window.showErrorMessage(`Error generating DDL: ${f.message}`)}}),l.commands.registerCommand("netezza.revealInSchema",async d=>{let f=l.window.setStatusBarMessage(`$(loading~spin) Revealing ${d.name} in schema...`);try{let E,v=l.window.activeTextEditor;if(v&&v.document.languageId==="sql"&&(E=e.getConnectionForExecution(v.document.uri.toString())),E||(E=d.connectionName),E||(E=e.getActiveConnectionName()||void 0),!E){f.dispose(),l.window.showWarningMessage("No active connection. Please select a connection first.");return}if(!await e.getConnectionString(E)){f.dispose(),l.window.showWarningMessage("Not connected to database");return}let T=d.name,S=d.objType,b=S?[S]:["TABLE","VIEW","EXTERNAL TABLE","PROCEDURE","FUNCTION","SEQUENCE","SYNONYM"];if(S==="COLUMN"){if(!d.parent){f.dispose(),l.window.showWarningMessage("Cannot find column without parent table");return}T=d.parent}if(d.database&&d.schema&&(S==="TABLE"||!S)){let N=`${d.database}.${d.schema}.${T}`,R=t.findTableId(E,N);if(R!==void 0){let{SchemaItem:$}=await Promise.resolve().then(()=>(Pe(),et)),O=new $(T,l.TreeItemCollapsibleState.Collapsed,"netezza:TABLE",d.database,"TABLE",d.schema,R,void 0,E);await c.reveal(O,{select:!0,focus:!0,expand:!0}),f.dispose(),l.window.setStatusBarMessage(`$(check) Found ${T} in ${d.database}.${d.schema} (cached)`,3e3);return}}let I=[];if(d.database)I=[d.database];else{let N=await e.getCurrentDatabase(E);if(N)I=[N];else{let R=await F(u,"SELECT DATABASE FROM system.._v_database ORDER BY DATABASE",!0,E,e);if(!R){f.dispose();return}I=JSON.parse(R).map(O=>O.DATABASE)}}for(let N of I)try{let R=S==="COLUMN"?["TABLE","VIEW","EXTERNAL TABLE"]:b;for(let $ of R){let O=`SELECT OBJNAME, OBJTYPE, SCHEMA, OBJID FROM ${N}.._V_OBJECT_DATA WHERE UPPER(OBJNAME) = UPPER('${T.replace(/'/g,"''")}') AND UPPER(OBJTYPE) = UPPER('${$}') AND DBNAME = '${N}'`;d.schema&&(O+=` AND UPPER(SCHEMA) = UPPER('${d.schema.replace(/'/g,"''")}')`);let W=await F(u,O,!0,E,e);if(W){let K=JSON.parse(W);if(K.length>0){let j=K[0],{SchemaItem:se}=await Promise.resolve().then(()=>(Pe(),et)),Ce=new se(j.OBJNAME,l.TreeItemCollapsibleState.Collapsed,`netezza:${j.OBJTYPE}`,N,j.OBJTYPE,j.SCHEMA,j.OBJID,void 0,E);await c.reveal(Ce,{select:!0,focus:!0,expand:!0}),f.dispose(),l.window.setStatusBarMessage(`$(check) Found ${T} in ${N}.${j.SCHEMA}`,3e3);return}}}}catch(R){console.log(`Error searching in ${N}:`,R)}f.dispose(),l.window.showWarningMessage(`Could not find ${S||"object"} ${T}`)}catch(E){f.dispose(),l.window.showErrorMessage(`Error revealing item: ${E.message}`)}}),l.commands.registerCommand("netezza.showQueryHistory",()=>{l.commands.executeCommand("netezza.queryHistory.focus")}),l.commands.registerCommand("netezza.clearQueryHistory",async()=>{let{QueryHistoryManager:d}=await Promise.resolve().then(()=>(_e(),Et)),f=new d(u);await l.window.showWarningMessage("Are you sure you want to clear all query history?",{modal:!0},"Clear All")==="Clear All"&&(await f.clearHistory(),h.refresh(),l.window.showInformationMessage("Query history cleared"))})),u.subscriptions.push(l.languages.registerDocumentLinkProvider({language:"sql"},new Fe)),u.subscriptions.push(l.languages.registerFoldingRangeProvider({language:"sql"},new ze)),u.subscriptions.push(l.commands.registerCommand("netezza.jumpToSchema",async()=>{let d=l.window.activeTextEditor;if(!d)return;let f=d.document,E=d.selection,v=f.offsetAt(E.active),C=re.getObjectAtPosition(f.getText(),v);C?l.commands.executeCommand("netezza.revealInSchema",C):l.window.showWarningMessage("No object found at cursor")}));let L=l.commands.registerCommand("netezza.runQuery",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.document,E=d.selection,v=f.getText(),C=f.uri.toString(),T=[];if(E.isEmpty){let b=f.offsetAt(E.active),I=re.getStatementAtPosition(v,b);if(I){T=[I.sql];let N=f.positionAt(I.start),R=f.positionAt(I.end);d.selection=new l.Selection(N,R)}else{l.window.showWarningMessage("No SQL statement found at cursor");return}}else{let b=f.getText(E);if(!b.trim()){l.window.showWarningMessage("No SQL query selected");return}T=re.splitStatements(b)}if(T.length===0)return;let S=T.length===1?T[0].trim():null;if(S){let b=S.split(/\s+/),I=b[0]||"",N=/python(\.exe)?$/i.test(I)&&b.length>=2&&b[1].toLowerCase().endsWith(".py"),R=I.toLowerCase().endsWith(".py");if(N||R){let O=l.workspace.getConfiguration("netezza").get("pythonPath")||"python",W="";if(N){let j=b[0],se=b[1],Ce=b.slice(2);W=m(j,se,Ce)}else{let j=I,se=b.slice(1);W=m(O,j,se)}let K=l.window.createTerminal({name:"Netezza: Script"});K.show(!0),K.sendText(W,!0),l.window.showInformationMessage(`Running script: ${W}`);return}}try{let b=await Ze(u,T,e,C);n.updateResults(b,C,!1),l.commands.executeCommand("netezza.results.focus")}catch(b){l.window.showErrorMessage(`Error executing query: ${b.message}`)}}),z=l.commands.registerCommand("netezza.runQueryBatch",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.document,E=d.selection,v=f.uri.toString(),C;if(E.isEmpty?C=f.getText():C=f.getText(E),!C.trim()){l.window.showWarningMessage("No SQL query to execute");return}let S=C.trim().split(/\s+/),b=S[0]||"",I=/python(\.exe)?$/i.test(b)&&S.length>=2&&S[1].toLowerCase().endsWith(".py"),N=b.toLowerCase().endsWith(".py");if(I||N){let $=l.workspace.getConfiguration("netezza").get("pythonPath")||"python",O="";if(I){let K=S[0],j=S[1],se=S.slice(2);O=m(K,j,se)}else{let K=b,j=S.slice(1);O=m($,K,j)}let W=l.window.createTerminal({name:"Netezza: Script"});W.show(!0),W.sendText(O,!0),l.window.showInformationMessage(`Running script: ${O}`);return}try{let{runQueryRaw:R}=await Promise.resolve().then(()=>(fe(),It)),$=await R(u,C,!1,e,void 0,v);$&&(n.updateResults([$],v,!1),l.commands.executeCommand("netezza.results.focus"))}catch(R){l.window.showErrorMessage(`Error executing query: ${R.message}`)}});u.subscriptions.push(z);let _=l.window.createOutputChannel("Netezza"),k=(d,f)=>{let E=Date.now()-f;_.appendLine(`[${new Date().toLocaleTimeString()}] ${d} completed in ${E}ms`),_.show(!0)},H=l.commands.registerCommand("netezza.exportToXlsb",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.selection,E=f.isEmpty?d.document.getText():d.document.getText(f);if(!E.trim()){l.window.showWarningMessage("No SQL query to export");return}let v=await l.window.showSaveDialog({filters:{"Excel Binary Workbook":["xlsb"]},saveLabel:"Export to XLSB"});if(!v)return;let C=Date.now();try{let T=d.document.uri.toString(),S=e.getConnectionForExecution(T),b=await e.getConnectionString(S);if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Exporting to XLSB...",cancellable:!1},async I=>{let{exportQueryToXlsb:N}=await Promise.resolve().then(()=>(ge(),me)),R=await N(b,E,v.fsPath,!1,$=>{I.report({message:$}),_.appendLine(`[XLSB Export] ${$}`)});if(!R.success)throw new Error(R.message)}),k("Export to XLSB",C),l.window.showInformationMessage(`Results exported to ${v.fsPath}`)}catch(T){l.window.showErrorMessage(`Error exporting to XLSB: ${T.message}`)}}),Y=l.commands.registerCommand("netezza.exportToCsv",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.selection,E=f.isEmpty?d.document.getText():d.document.getText(f);if(!E.trim()){l.window.showWarningMessage("No SQL query to export");return}let v=await l.window.showSaveDialog({filters:{"CSV Files":["csv"]},saveLabel:"Export to CSV"});if(!v)return;let C=Date.now();try{let T=d.document.uri.toString(),S=e.getConnectionForExecution(T),b=await e.getConnectionString(S);if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Exporting to CSV...",cancellable:!1},async I=>{let{exportToCsv:N}=await Promise.resolve().then(()=>(Ht(),Ut));await N(u,b,E,v.fsPath,I)}),k("Export to CSV",C),l.window.showInformationMessage(`Results exported to ${v.fsPath}`)}catch(T){l.window.showErrorMessage(`Error exporting to CSV: ${T.message}`)}}),ne=l.commands.registerCommand("netezza.copyXlsbToClipboard",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.selection,E=f.isEmpty?d.document.getText():d.document.getText(f);if(!E.trim()){l.window.showWarningMessage("No SQL query to export");return}try{let v=d.document.uri.toString(),C=e.getConnectionForExecution(v),T=await e.getConnectionString(C);if(!T)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let S=Date.now();if(await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Exporting to XLSB and copying to clipboard...",cancellable:!1},async I=>{let{exportQueryToXlsb:N,getTempFilePath:R}=await Promise.resolve().then(()=>(ge(),me)),$=R(),O=await N(T,E,$,!0,W=>{I.report({message:W}),_.appendLine(`[XLSB Clipboard] ${W}`)});if(!O.success)throw new Error(O.message);if(!O.details?.clipboard_success)throw new Error("Failed to copy file to clipboard")}),k("Copy XLSB to Clipboard",S),await l.window.showInformationMessage("Excel file copied to clipboard! You can now paste it into Excel or Windows Explorer.","Show Temp Folder","OK")==="Show Temp Folder"){let I=require("os").tmpdir();await l.env.openExternal(l.Uri.file(I))}}catch(v){l.window.showErrorMessage(`Error copying XLSB to clipboard: ${v.message}`)}}),J=l.commands.registerCommand("netezza.exportToXlsbAndOpen",async()=>{let d=l.window.activeTextEditor;if(!d){l.window.showErrorMessage("No active editor found");return}let f=d.selection,E=f.isEmpty?d.document.getText():d.document.getText(f);if(!E.trim()){l.window.showWarningMessage("No SQL query to export");return}let v=await l.window.showSaveDialog({filters:{"Excel Binary Workbook":["xlsb"]},saveLabel:"Export to XLSB and Open"});if(!v)return;let C=Date.now();try{let T=d.document.uri.toString(),S=e.getConnectionForExecution(T),b=await e.getConnectionString(S);if(!b)throw new Error("Connection not configured. Please connect via Netezza: Connect...");await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Exporting to XLSB and opening...",cancellable:!1},async I=>{let{exportQueryToXlsb:N}=await Promise.resolve().then(()=>(ge(),me)),R=await N(b,E,v.fsPath,!1,$=>{I.report({message:$}),_.appendLine(`[XLSB Export] ${$}`)});if(!R.success)throw new Error(R.message)}),k("Export to XLSB and Open",C),await l.env.openExternal(v),l.window.showInformationMessage(`Results exported and opened: ${v.fsPath}`)}catch(T){l.window.showErrorMessage(`Error exporting to XLSB: ${T.message}`)}}),de=l.commands.registerCommand("netezza.importClipboard",async()=>{try{let f=l.window.activeTextEditor?.document?.uri?.toString(),E=e.getConnectionForExecution(f),v=await e.getConnectionString(E);if(!v)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let C=await l.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:I=>!I||I.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(I.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(C===void 0)return;let T;if(!C||C.trim().length===0)try{let N=await F(u,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0,E,e);if(N){let R=JSON.parse(N);if(R&&R.length>0){let $=R[0].CURRENT_CATALOG||"SYSTEM",O=R[0].CURRENT_SCHEMA||"ADMIN",K=new Date().toISOString().slice(0,10).replace(/-/g,""),j=Math.floor(Math.random()*1e4).toString().padStart(4,"0");T=`${$}.${O}.IMPORT_${K}_${j}`,l.window.showInformationMessage(`Auto-generated table name: ${T}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(I){l.window.showErrorMessage(`Error getting current database/schema: ${I.message}`);return}else T=C.trim();let S=await l.window.showQuickPick([{label:"Auto-detect",description:"Automatically detect clipboard format (text or Excel XML)",value:null},{label:"Excel XML Spreadsheet",description:"Force Excel XML format processing",value:"XML Spreadsheet"},{label:"Plain Text",description:"Force plain text processing with delimiter detection",value:"TEXT"}],{placeHolder:"Select clipboard data format"});if(!S)return;let b=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Importing clipboard data...",cancellable:!1},async I=>{let{importClipboardDataToNetezza:N}=await Promise.resolve().then(()=>(tn(),en)),R=await N(T,v,S.value,{},$=>{I.report({message:$}),_.appendLine(`[Clipboard Import] ${$}`)});if(!R.success)throw new Error(R.message);R.details&&(_.appendLine(`[Clipboard Import] Rows processed: ${R.details.rowsProcessed}`),_.appendLine(`[Clipboard Import] Columns: ${R.details.columns}`),_.appendLine(`[Clipboard Import] Format: ${R.details.format}`))}),k("Import Clipboard Data",b),l.window.showInformationMessage(`Clipboard data imported successfully to table: ${T}`)}catch(d){l.window.showErrorMessage(`Error importing clipboard data: ${d.message}`)}}),te=l.commands.registerCommand("netezza.importData",async()=>{try{let f=l.window.activeTextEditor?.document?.uri?.toString(),E=e.getConnectionForExecution(f),v=await e.getConnectionString(E);if(!v)throw new Error("Connection not configured. Please connect via Netezza: Connect...");let C=await l.window.showOpenDialog({canSelectFiles:!0,canSelectFolders:!1,canSelectMany:!1,filters:{"Data Files":["csv","txt","xlsx","xlsb","json"],"CSV Files":["csv"],"Excel Files":["xlsx","xlsb"],"Text Files":["txt"],"JSON Files":["json"],"All Files":["*"]},openLabel:"Select file to import"});if(!C||C.length===0)return;let T=C[0].fsPath,S=await l.window.showInputBox({prompt:"Enter target table name (leave empty for auto-generated name)",placeHolder:"e.g. my_schema.my_table or leave empty",validateInput:R=>!R||R.trim().length===0||/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(R.trim())?null:"Invalid table name format. Use: [database.]schema.table"});if(S===void 0)return;let b;if(!S||S.trim().length===0)try{let $=await F(u,"SELECT CURRENT_CATALOG, CURRENT_SCHEMA",!0,E,e);if($){let O=JSON.parse($);if(O&&O.length>0){let W=O[0].CURRENT_CATALOG||"SYSTEM",K=O[0].CURRENT_SCHEMA||"ADMIN",se=new Date().toISOString().slice(0,10).replace(/-/g,""),Ce=Math.floor(Math.random()*1e4).toString().padStart(4,"0");b=`${W}.${K}.IMPORT_${se}_${Ce}`,l.window.showInformationMessage(`Auto-generated table name: ${b}`)}else throw new Error("Could not determine current database/schema")}else throw new Error("Could not determine current database/schema")}catch(R){l.window.showErrorMessage(`Error getting current database/schema: ${R.message}`);return}else b=S.trim();let I=await l.window.showQuickPick([{label:"Default Import",description:"Use default settings",value:{}},{label:"Custom Options",description:"Configure import settings (coming soon)",value:null}],{placeHolder:"Select import options"});if(!I)return;if(I.value===null){l.window.showInformationMessage("Custom options will be available in future version");return}let N=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Importing data...",cancellable:!1},async R=>{let{importDataToNetezza:$}=await Promise.resolve().then(()=>(mt(),Gt)),O=await $(T,b,v,I.value||{},W=>{R.report({message:W}),_.appendLine(`[Import] ${W}`)});if(!O.success)throw new Error(O.message);O.details&&(_.appendLine(`[Import] Rows processed: ${O.details.rowsProcessed}`),_.appendLine(`[Import] Columns: ${O.details.columns}`),_.appendLine(`[Import] Delimiter: ${O.details.detectedDelimiter}`))}),k("Import Data",N),l.window.showInformationMessage(`Data imported successfully to table: ${b}`)}catch(d){l.window.showErrorMessage(`Error importing data: ${d.message}`)}}),Je=l.commands.registerCommand("netezza.exportCurrentResultToXlsbAndOpen",async(d,f)=>{try{if(!d){l.window.showErrorMessage("No data to export");return}let E=require("os"),v=require("path"),C=new Date().toISOString().replace(/[:.]/g,"-"),T=v.join(E.tmpdir(),`netezza_results_${C}.xlsb`),S=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Creating Excel file...",cancellable:!1},async I=>{let{exportCsvToXlsb:N}=await Promise.resolve().then(()=>(ge(),me)),R=await N(d,T,!1,{source:"Query Results Panel",sql:f},$=>{I.report({message:$}),_.appendLine(`[CSV to XLSB] ${$}`)});if(!R.success)throw new Error(R.message)});let b=Date.now()-S;_.appendLine(`[${new Date().toLocaleTimeString()}] Export Current Result to Excel completed in ${b}ms`),await l.env.openExternal(l.Uri.file(T)),l.window.showInformationMessage(`Results exported and opened: ${T}`)}catch(E){l.window.showErrorMessage(`Error exporting to Excel: ${E.message}`)}}),Xe=l.commands.registerCommand("netezza.copyCurrentResultToXlsbClipboard",async(d,f)=>{try{if(!d){l.window.showErrorMessage("No data to copy");return}let{getTempFilePath:E}=await Promise.resolve().then(()=>(ge(),me)),v=E(),C=Date.now();await l.window.withProgress({location:l.ProgressLocation.Notification,title:"Copying to clipboard as Excel...",cancellable:!1},async S=>{let{exportCsvToXlsb:b}=await Promise.resolve().then(()=>(ge(),me)),I=await b(d,v,!0,{source:"Query Results Panel",sql:f},N=>{S.report({message:N}),_.appendLine(`[CSV to Clipboard] ${N}`)});if(!I.success)throw new Error(I.message);if(!I.details?.clipboard_success)throw new Error("Failed to copy file to clipboard")});let T=Date.now()-C;_.appendLine(`[${new Date().toLocaleTimeString()}] Copy Current Result to Clipboard completed in ${T}ms`),l.window.showInformationMessage("Results copied to clipboard as Excel table! You can paste in Excel or Explorer.")}catch(E){l.window.showErrorMessage(`Error copying to clipboard: ${E.message}`)}});u.subscriptions.push(L),u.subscriptions.push(H),u.subscriptions.push(Y),u.subscriptions.push(ne),u.subscriptions.push(J),u.subscriptions.push(Je),u.subscriptions.push(Xe),u.subscriptions.push(de),u.subscriptions.push(te);let Qn=l.workspace.onWillSaveTextDocument(async d=>{}),rn=l.commands.registerCommand("netezza.smartPaste",async()=>{try{let d=l.window.activeTextEditor;if(!d)return;let E=l.workspace.getConfiguration("netezza").get("pythonPath")||"python",v=on.join(u.extensionPath,"python","check_clipboard_format.py"),C=require("child_process");if(await new Promise(S=>{let b=C.spawn(E,[v]);b.on("close",I=>{S(I===1)}),b.on("error",()=>{S(!1)})})){let S=await l.window.showQuickPick([{label:"\u{1F4CA} import to Netezza table",description:'Detected "XML Spreadsheet" format - import data to database',value:"import"},{label:"\u{1F4DD} Paste as text",description:"Paste clipboard content as plain text",value:"paste"}],{placeHolder:'Detected "XML Spreadsheet" format in clipboard - choose an action'});if(S?.value==="import")l.commands.executeCommand("netezza.importClipboard");else if(S?.value==="paste"){let b=await l.env.clipboard.readText(),I=d.selection;await d.edit(N=>{N.replace(I,b)})}}else{let S=await l.env.clipboard.readText(),b=d.selection;await d.edit(I=>{I.replace(b,S)})}}catch(d){l.window.showErrorMessage(`Error during paste: ${d.message}`)}}),an=l.workspace.onDidChangeTextDocument(async d=>{if(d.document.languageId!=="sql"&&d.document.languageId!=="mssql"||d.contentChanges.length!==1)return;let f=d.contentChanges[0];if(f.text!==" ")return;let E=l.window.activeTextEditor;if(!E||E.document!==d.document)return;let C=d.document.lineAt(f.range.start.line).text,T=new Map([["SX","SELECT"],["WX","WHERE"],["GX","GROUP BY"],["HX","HAVING"],["OX","ORDER BY"],["FX","FROM"],["JX","JOIN"],["LX","LIMIT"],["IX","INSERT INTO"],["UX","UPDATE"],["DX","DELETE FROM"],["CX","CREATE TABLE"]]);for(let[S,b]of T)if(new RegExp(`\\b${S}\\s$`,"i").test(C)){let N=C.toUpperCase().lastIndexOf(S.toUpperCase());if(N>=0){let R=new l.Position(f.range.start.line,N),$=new l.Position(f.range.start.line,N+S.length+1);await E.edit(O=>{O.replace(new l.Range(R,$),b+" ")}),["SELECT","FROM","JOIN"].includes(b)&&setTimeout(()=>{l.commands.executeCommand("editor.action.triggerSuggest")},100);break}}});u.subscriptions.push(rn),u.subscriptions.push(an);let cn=new Be(u,t,e);u.subscriptions.push(l.languages.registerCompletionItemProvider(["sql","mssql"],cn,"."," ")),u.subscriptions.push(l.commands.registerCommand("netezza.clearAutocompleteCache",async()=>{await l.window.showWarningMessage("Are you sure you want to clear the autocomplete cache? This will remove all cached databases, schemas, tables, and columns.",{modal:!0},"Clear Cache")==="Clear Cache"&&(await t.clearCache(),l.window.showInformationMessage("Autocomplete cache cleared successfully. Cache will be rebuilt on next use."))}))}function Yn(){}0&&(module.exports={activate,deactivate});
