from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "changeme"

class HelloWorldExample:

    def __init__(self, URI, USER, PASSWORD):
        self.driver = GraphDatabase.driver(URI, auth=(USER,PASSWORD))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]


if __name__ == "__main__":
    greeter = HelloWorldExample(URI, USER, PASSWORD)
    greeter.print_greeting("hello, world")
    greeter.close()