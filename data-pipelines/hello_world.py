import luigi
from time import sleep
import os

class MakeDirectory(luigi.Task):
    path = luigi.Parameter()


    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)


class HelloTask(luigi.Task):
    path = luigi.Parameter()
    word = luigi.Parameter()
    def run(self):
        with open(self.path, 'w') as hello_file:
            hello_file.write(self.word)
            hello_file.close()


    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path))
        ]

    def output(self):
        return luigi.LocalTarget(self.path)


class WorldTask(luigi.Task):
    path = luigi.Parameter()
    word = luigi.Parameter()
    def run(self):
        with open(self.path, 'w') as hello_file:
            hello_file.write(self.word)
            hello_file.close()


    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path))
        ]

    def output(self):
        return luigi.LocalTarget(self.path)

class HelloWorldTask(luigi.Task):
    id = luigi.Parameter(default='test')
    first = luigi.Parameter(default='first')
    second = luigi.Parameter(default='second')

    def run(self):
        with open(self.input()[0].path, 'r') as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path,'r') as world_file:
            world = world_file.read()
        with open(self.output().path, 'w') as output_file:
            content = '{} {}!'.format(hello, world)
            output_file.write(content)
            output_file.close()


    def requires(self):
        return [HelloTask(
            path = 'results/{}/first_name.txt'.format(self.id),
            word = self.first
        ), WorldTask(
            path = 'results/{}/second_name.txt'.format(self.id),
            word = self.second
        )]

    def output(self):
        path = 'results/{}/cohort.txt'.format(self.id)
        return luigi.LocalTarget(path)

if __name__ == '__main__':
    luigi.run()