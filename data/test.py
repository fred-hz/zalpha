class test:
    def __init__(self):
        print(1)

    def one(self):
        self.x = 2

    def two(self):
        print(self.x)

y = test()
y.one()
y.two()