import random
import numpy as np
import networkx as nx


def generate_arrival_times(lam, size):  # alternative function will keep the arrivals fixed
    arrivals = np.random.poisson(lam, size)

    return arrivals

class Resource:
    def __init__(self, name):
        self.name = name
        self.capacity = 1
        self.queue = []


class Task:
    def __init__(self, name, job_t, duration):
        self.name = name
        self.resource = -1
        self.job_type = job_type
        self.duration = duration
        self.arrival_time = -1
        self.is_finished = False


class JobType:
    def __init__(self, name, job_arrival_lambda, job_duration_power, num_tasks):
        self.name = name
        self.job_arrival_lambda = job_arrival_lambda
        self.job_duration_power = job_duration_power
        self.num_tasks= num_tasks
        self.queue = []


class ResourceAllocationSimulation:
    def __init__(self, total_duration, mean_duration_power, job_type_distributions, num_workers, graph):
        self.total_duration = total_duration

        self.mean_duration_power = mean_duration_power
        self.job_type_distributions = job_type_distributions
        self.num_workers = num_workers
        self.resources = []
        self.tasks = []
        self.current_time = 0
        self.graph = graph
        self.completed_tasks = []
        self.total_tasks = 0

    def add_resource(self, resource):
        self.resources.append(resource)

    def generate_tasks(self):
        cnt = 0
        for job_type, distribution in self.job_type_distributions.items():
            num_tasks = distribution.num_tasks  # Fix the number of tasks
            arrival_times = generate_arrival_times(distribution.job_arrival_lambda, self.total_duration)

            print(distribution.job_arrival_lambda, arrival_times)
            for ind, at in enumerate(arrival_times):
                if at == 0:
                    continue
                self.total_tasks += at
                for _ in range(at):
                    task_name = f"{job_type} - Task {cnt}"
                    cnt += 1
                    job_duration = int(np.random.exponential(1/distribution.job_duration_power))
                    task = Task(task_name, job_type, job_duration)
                    self.tasks.append(task)
                    task.arrival_time = ind

    def run_simulation(self):
        self.tasks.sort(key=lambda x : -x.arrival_time)
        while self.current_time <= self.total_duration or len( self.completed_tasks) < self.total_tasks:
            available_workers = min(len(self.resources), self.num_workers)
            while True:
                if self.tasks:
                    task = self.tasks[-1]
                    if self.current_time >= task.arrival_time:
                        self.tasks.pop()
                        random_resource = random.choice(list(self.graph.neighbors(task.job_type))) #change to something more inteligent.
                        resource_name = random_resource
                        resource = next((r for r in self.resources if r.name == resource_name), None)
                        if resource:
                            resource.queue.append(task)
                    else:
                        break
                else:
                    break

            for resource in self.resources:
                if resource.queue:
                    task = resource.queue[0]
                    print(
                        f"Time: {self.current_time}s - Allocating resource '{resource.name}' to task '{task.name}' from the queue")
                    task.duration -= 1
                    if task.duration <= 0:
                        task.is_finished = True
                        self.completed_tasks.append(task)
                        resource.queue.pop(0)

            self.current_time += 1

    def generate_report(self):
        total_finished = len(self.completed_tasks)
        total_unfinished = len(self.tasks)
        total_time = self.current_time
        print("Simulation Report:")
        print(f"Total Tasks: {self.total_tasks}")
        print(f"Finished Tasks: {total_finished}")
        print(f"Unfinished Tasks: {total_unfinished}")
        print(f"Total Time: {total_time}s")


# Define the job type distributions
job_type_distributions = {
    'Job Type 1': JobType('Job Type 1', 0.4, 1/14, 4),
    'Job Type 2': JobType('Job Type 2', 0.2, 1/14, 4),
    'Job Type 3': JobType('Job Type 3', 0.15, 1/14, 4),
    'Job Type 4': JobType('Job Type 4', 0.15, 1/14, 4),
}

graph = nx.Graph()
job_types = {}
for job_type, distribution in job_type_distributions.items():
    job_type_instance = JobType(job_type, distribution.job_arrival_lambda, distribution.job_duration_power, distribution.num_tasks)
    job_types[job_type] = job_type_instance
    graph.add_node(job_type_instance.name, bipartite=0)  # Use the name of the JobType instance as the node
graph.add_nodes_from(range(1, 16), bipartite=1)
for job_type, _ in job_type_distributions.items():
    for resource in range(1, 16):
        graph.add_edge(job_type, resource)

# Create the simulation
simulation = ResourceAllocationSimulation(60, 1/30, job_types, 15, graph) #duration mean is 30

# Create resources
for i in range(1, 16):
    resource = Resource(i)
    simulation.add_resource(resource)

simulation.generate_tasks()
# Run the simulation
simulation.run_simulation()

# Generate the report
simulation.generate_report()
