from pydantic import BaseModel,ConfigDict,Field
class Strict(BaseModel):model_config=ConfigDict(extra='forbid',allow_inf_nan=False)
class Span(Strict):
    trace_id:str=Field(pattern=r'^[0-9a-f]{32}$')
    span_id:str=Field(pattern=r'^[0-9a-f]{16}$')
    parent_id:str|None=Field(default=None,pattern=r'^[0-9a-f]{16}$')
    service:str=Field(pattern=r'^[A-Za-z0-9_-]{1,64}$')
    operation:str=Field(min_length=1,max_length=100)
    start_ms:int=Field(ge=0)
    duration_ms:float=Field(ge=0,le=600000)
    status:int=Field(ge=100,le=599)
class Runtime(Strict):
    heap_bytes:int=Field(ge=0,default=0)
    peak_rss_bytes:int=Field(ge=0,default=0)
    cpu_cores:float=Field(ge=0,le=1024,default=0)
class Batch(Strict):
    batch_id:str=Field(pattern=r'^[0-9a-f]{32}$')
    spans:list[Span]=Field(min_length=1,max_length=1000)
    runtime:Runtime=Field(default_factory=Runtime)
