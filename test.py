def foo():
  for i in range(10):
    yield i


print(list(foo()))