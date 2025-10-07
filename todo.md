你要完成一个并行执行器项目，要求如下：
同步和异步的任务执行包装 要求提供两个版本 异步使用asyncio 同步使用threading
```python
# def background_task(fn: Callable):
#     def wrapper(*args, **kwargs):
#         thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
#         thread.daemon = True  # 设置为守护线程，主线程结束时自动退出
#         thread.start()
#         return thread
    
#     return wrapper
def background_task(fn: Callable):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.create_task(
            fn(*args, **kwargs)
        )
    
    return wrapper

class Test(AsyncIOEventEmitter):
    def __init__(self):
        super().__init__()

    @background_task
    async def test(self):
        await asyncio.sleep(1)  # 模拟耗时操作
        data = 'data'
        print(f"test emit data: {data}")
        self.emit('test', data)

async def process_data(data):
    await asyncio.sleep(1)  # 模拟异步处理
    print(f"process: {data}")

def main():
    asyncio.run(main_async())

async def main_async():
    if True:
        t = Test()
        t.once('test', process_data)  # 注册事件监听器
        t.test()  # 触发事件
    print("Test over")
    
    # 主线程等待足够时间，确保子线程执行完成
    await asyncio.sleep(4)
    print("main over")

if __name__ == "__main__":
    main()
```
要求 我能够批量提交要并行计算的任务 并能够等待他们同时完成或者不等待完成（两种方式可选）
我可以通过pyee去监听任务开始or完成 也可以传入回调让任务完成的时候自动执行（这两个应该是不互斥的）
回调函数的入产应该就是任务的返回值： 
所以你应该定义泛型 如何描绘这种数据结构 Generic[T] 确保mypy检查通过
```python
async def task() -> str: ...
async def callback(data: str): ...
```
最后要进行丰富的测试和用例 来验证任务完成


# 编码规范
- 不能用dict传递数据 要用pydantic的BaseModel 这是为了可读性和可维护性
- 所有的函数都不能用get_xxx 请用python的property 或者直接用资源名字代替 get_resource (但是可以用set_xxx)
- 所有的函数都要typing 注解 必要的时候要加入Annotation
# 项目规范
- 用uv管理项目 如果要增加库 用uv add xxx