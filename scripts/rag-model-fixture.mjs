// Deterministic OpenAI-shaped protocol fixture used ONLY by disposable RAG CI.
// No model inference, external requests, credentials or market facts.
import http from 'node:http';
const counts={embedding:0,extraction:0,keywords:0};
const json=(r,status,body)=>{r.writeHead(status,{'Content-Type':'application/json'});r.end(JSON.stringify(body))};
http.createServer(async(req,res)=>{
 if(req.url==='/healthz'||req.url==='/stats')return json(res,200,counts);
 const chunks=[];let size=0;
 for await(const c of req){size+=c.length;if(size>2*1024*1024){json(res,413,{error:'fixture input limit'});req.destroy();return}chunks.push(c)}
 let body;try{body=JSON.parse(Buffer.concat(chunks))}catch{return json(res,400,{error:'invalid fixture JSON'})}
 if(req.url==='/v1/embeddings'){
  counts.embedding++;
  const input=Array.isArray(body.input)?body.input:[body.input];
  return json(res,200,{object:'list',model:'controlled-embedding',data:input.map((_,index)=>({object:'embedding',index,embedding:[1,0.1,0.2,0.3]})),usage:{prompt_tokens:input.length,total_tokens:input.length}});
 }
 if(req.url==='/v1/chat/completions'){
  const prompt=JSON.stringify(body.messages??[]);let content;
  if(prompt.includes('high_level_keywords')&&prompt.includes('low_level_keywords')){
   counts.keywords++;content=JSON.stringify({high_level_keywords:['inventory'],low_level_keywords:['Methanol']});
  }else{
   counts.extraction++;
   content='entity<|#|>Methanol<|#|>CONCEPT<|#|>Methanol is the subject of a controlled inventory fixture.\nentity<|#|>Inventory<|#|>CONCEPT<|#|>Inventory is a controlled test measurement.\nrelation<|#|>Methanol<|#|>Inventory<|#|>inventory<|#|>The fixture describes methanol inventory.\n<|COMPLETE|>';
  }
  return json(res,200,{id:'fixture-completion',object:'chat.completion',created:1,model:'controlled-extraction',choices:[{index:0,message:{role:'assistant',content},finish_reason:'stop'}],usage:{prompt_tokens:10,completion_tokens:10,total_tokens:20}});
 }
 json(res,404,{error:'fixture route not found'});
}).listen(8113,'0.0.0.0');
