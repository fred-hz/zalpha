class A(object):
    def __init__(self, a):
        self.a = a
        self.a.append(1)

a = []
obj = A(a)
print(a)