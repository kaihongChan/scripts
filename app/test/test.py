import sys

sys.path.append("/app/scripts/")
from app.base import Base


class Test(Base):
    def __init__(self):
        super().__init__()

    def run(self):
        print(222222222)
        return


if __name__ == '__main__':
    Test().run()
