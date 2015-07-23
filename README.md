# transaction-propagator-jta

JTA based implementation of [transaction-propagator-api][1].

## Usage examples

    TransactionPropagator propagator =
        new JTATransactionPropagator(transactionManager);

    Integer result = propagator.requiresNew(() -> {
        // Do some stuff in the new transaction
    });

## Download

The artifact is available on [maven-central][2].

[1]: https://github.com/everit-org/transaction-propagator-api
[2]: http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22org.everit.transaction.propagator.jta%22

