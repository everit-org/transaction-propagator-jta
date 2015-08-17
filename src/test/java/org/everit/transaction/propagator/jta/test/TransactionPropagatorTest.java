/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.transaction.propagator.jta.test;

import java.io.IOException;
import java.util.Arrays;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.osgi.transaction.propagator.jta.JTATransactionPropagator;
import org.everit.transaction.propagator.TransactionPropagator;
import org.everit.transaction.unchecked.UncheckedRollbackException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionPropagatorTest {

  private static class Thrower {
    public static void spit(final Throwable exception) {
      class EvilThrower<T extends Throwable> {
        @SuppressWarnings("unchecked")
        private void sneakyThrow(final Throwable exception) throws T {
          throw (T) exception;
        }
      }
      new EvilThrower<RuntimeException>().sneakyThrow(exception);
    }
  }

  /**
   * The {@link TransactionManager} instance.
   */
  private TransactionManager transactionManager;

  /**
   * The {@link TransactionPropagator} instance.
   */
  private TransactionPropagator transactionPropagator;

  private void assertTransactionStatus(final int expected) {
    int status = getStatus();
    Assert.assertEquals(expected, status);
  }

  @Before
  public void before() {
    try {
      transactionManager = new GeronimoTransactionManager();
    } catch (XAException e) {
      throw new RuntimeException(e);
    }
    transactionPropagator = new JTATransactionPropagator(transactionManager);
  }

  private void enlistResource(final XAResource resource) {
    Transaction transaction = getTransaction();
    try {
      transaction.enlistResource(resource);
    } catch (IllegalStateException e) {
      throw new RuntimeException(e);
    } catch (RollbackException e) {
      throw new RuntimeException(e);
    } catch (SystemException e) {
      throw new RuntimeException(e);
    }
  }

  private int getStatus() {
    try {
      return transactionManager.getStatus();
    } catch (SystemException e) {
      throw new RuntimeException(e);
    }
  }

  private Transaction getTransaction() {
    try {
      return transactionManager.getTransaction();
    } catch (SystemException e) {
      throw new RuntimeException(e);
    }
  }

  protected void setTransactionManager(final TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  protected void setTransactionPropagator(final TransactionPropagator transactionPropagator) {
    this.transactionPropagator = transactionPropagator;
  }

  @Test
  public void test01RequiredNoTransactionBeforeSucess() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);
      return 1;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test02RequiredNoTransactionBeforeFail() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    try {
      transactionPropagator.required(() -> {
        enlistResource(lastTrStatus);
        throw new NumberFormatException();
      });
      Assert.fail("Exception should be thrown");
    } catch (NumberFormatException e) {
      Assert.assertEquals(Status.STATUS_ROLLEDBACK, lastTrStatus.getStatus());
    }
  }

  @Test
  public void test03RequiredOngoingTRSuccess() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    int result = transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);

      int innerResult = transactionPropagator.required(() -> {
        return 1;
      });

      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      return innerResult;
    });

    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
    Assert.assertEquals(1, result);
  }

  @Test
  public void test04RequiredOngoingTRFail() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    try {
      transactionPropagator.required(() -> {
        enlistResource(lastTrStatus);
        try {
          return (Integer) transactionPropagator.required(() -> {
            throw new NumberFormatException();
          });
        } catch (NumberFormatException e) {
          Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
          assertTransactionStatus(Status.STATUS_MARKED_ROLLBACK);
          throw e;
        }
      });
      Assert.fail("Exception should be thrown here");
    } catch (NumberFormatException e) {
      Assert.assertEquals(Status.STATUS_ROLLEDBACK, lastTrStatus.getStatus());
    }

  }

  @Test
  public void test05RequiredOngoingTRCaughedFail() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    try {
      transactionPropagator.required(() -> {
        enlistResource(lastTrStatus);
        try {
          return (Integer) transactionPropagator.required(() -> {
            throw new NumberFormatException();
          });
        } catch (NumberFormatException e) {
          Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
          assertTransactionStatus(Status.STATUS_MARKED_ROLLBACK);
        }
        return 1;
      });
      Assert.fail("IllegalstateException should have been thrown here");
    } catch (UncheckedRollbackException e) {
      Assert.assertTrue(e.getCause() instanceof RollbackException);
      Assert.assertEquals(Status.STATUS_ROLLEDBACK, lastTrStatus.getStatus());
    }
  }

  @Test
  public void test06RequiresNewNoTransactionSuccess() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    Integer result = transactionPropagator.requiresNew(() -> {
      enlistResource(lastTrStatus);
      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());
      return 1;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
    Assert.assertEquals(Status.STATUS_NO_TRANSACTION, getStatus());
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void test07RequiresNewOngoingTransactionSuccess() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      final RememberLastCallXAResource innerLastTrStatus = new RememberLastCallXAResource();
      enlistResource(lastTrStatus);
      Integer result = transactionPropagator.requiresNew(() -> {
        enlistResource(innerLastTrStatus);
        byte[] outerTrId = lastTrStatus.getXid().getGlobalTransactionId();
        byte[] innerTrId = innerLastTrStatus.getXid().getGlobalTransactionId();
        Assert.assertFalse(Arrays.equals(outerTrId, innerTrId));
        return 1;
      });
      Assert.assertEquals(Status.STATUS_COMMITTED, innerLastTrStatus.getStatus());
      Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());
      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      return result;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test08RequiresNewOngoingTransactionFailAndCatch() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.requiresNew(() -> {
      final RememberLastCallXAResource innerLastTrStatus = new RememberLastCallXAResource();
      enlistResource(lastTrStatus);
      try {
        transactionPropagator.requiresNew(() -> {
          enlistResource(innerLastTrStatus);
          throw new NumberFormatException();
        });
        Assert.fail("Code part should not be accessible");
      } catch (NumberFormatException e) {
        Assert.assertEquals(Status.STATUS_ROLLEDBACK, innerLastTrStatus.getStatus());
        Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());
        Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      }

      return 1;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test09MandatorySuccess() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);
      Integer result = transactionPropagator.mandatory(() -> {
        assertTransactionStatus(Status.STATUS_ACTIVE);
        return 1;
      });
      assertTransactionStatus(Status.STATUS_ACTIVE);
      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      return result;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test10MandatoryFailAsThereIsNoActiveTransaction() {
    try {
      transactionPropagator.mandatory((Runnable) null);
      Assert.fail("Should have thrown an exception");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Allowed status: active; Current status: no_transaction", e.getMessage());
    }
  }

  @Test
  public void test11MandatoryFailDueToInnerException() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    try {
      transactionPropagator.required(() -> {
        enlistResource(lastTrStatus);
        try {
          return (Integer) transactionPropagator.mandatory(() -> {
            assertTransactionStatus(Status.STATUS_ACTIVE);
            throw new NumberFormatException();
          });
        } catch (NumberFormatException e) {
          assertTransactionStatus(Status.STATUS_MARKED_ROLLBACK);
          throw e;
        }
      });
      Assert.fail("Exception should be thrown");
    } catch (NumberFormatException e) {
      Assert.assertEquals(Status.STATUS_ROLLEDBACK, lastTrStatus.getStatus());
    }
  }

  @Test
  public void test12NeverSuccess() {
    Integer result = transactionPropagator.never(() -> {
      Assert.assertEquals(Status.STATUS_NO_TRANSACTION, getStatus());
      return 1;
    });
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void test13NeverFailDueToOngoingTransaction() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);
      try {
        transactionPropagator.never(() -> {
          Assert.fail("Unreachable code");
          return null;
        });
      } catch (IllegalStateException e) {
        Assert.assertEquals("Allowed status: no_transaction; Current status: active",
            e.getMessage());
      }
      return null;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test14SupportsWithSuccessNoTransaction() {
    Integer result = transactionPropagator.supports(() -> {
      Assert.assertEquals(Status.STATUS_NO_TRANSACTION, getStatus());
      return 1;
    });
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void test15SupportsWithSuccessOngoingTransaciton() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    final RememberLastCallXAResource outerLastTrStatus = new RememberLastCallXAResource();
    Integer result = transactionPropagator.required(() -> {
      enlistResource(outerLastTrStatus);

      Integer innerResult = transactionPropagator.supports(() -> {
        enlistResource(lastTrStatus);
        Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());
        byte[] outerXID = outerLastTrStatus.getXid().getGlobalTransactionId();
        byte[] innerXID = lastTrStatus.getXid().getGlobalTransactionId();
        Assert.assertTrue(Arrays.equals(outerXID, innerXID));
        return 1;
      });

      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      return innerResult;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, outerLastTrStatus.getStatus());
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void test16SupportsWithExceptionNoTransaction() {
    try {
      transactionPropagator.supports(() -> {
        throw new NumberFormatException();
      });
      Assert.fail("Code should be unreachable");
    } catch (NumberFormatException e) {
      // Good
    }
  }

  @Test
  public void test17SupporstWithExceptionOngoingTransaction() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    try {
      transactionPropagator.required(() -> {
        enlistResource(lastTrStatus);
        try {
          transactionPropagator.supports(() -> {
            throw new NumberFormatException();
          });
          Assert.fail("Code should be unreachable");
        } catch (NumberFormatException e) {
          Assert.assertEquals(Status.STATUS_MARKED_ROLLBACK, getStatus());
        }
        return null;
      });
      Assert.fail("Code should be unreachable");
    } catch (UncheckedRollbackException e) {
      Assert.assertEquals(Status.STATUS_ROLLEDBACK, lastTrStatus.getStatus());
    }
  }

  @Test
  public void test18NotSupportedWithSuccessNoTransaction() {
    Integer result = transactionPropagator.notSupported(() -> {
      Assert.assertEquals(Status.STATUS_NO_TRANSACTION, getStatus());
      return 1;
    });
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void test19NotSupportedWithSuccessOngoingTransaction() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    final RememberLastCallXAResource lastTrStatus2 = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);
      byte[] trIdBefore = lastTrStatus.getXid().getGlobalTransactionId();

      Integer result = transactionPropagator.notSupported(() -> {
        Assert.assertEquals(Status.STATUS_NO_TRANSACTION, getStatus());
        return 1;
      });

      Assert.assertEquals(1, result.intValue());
      Assert.assertEquals(XAResourceStatus.STATUS_START, lastTrStatus.getStatus());
      Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());

      enlistResource(lastTrStatus2);
      byte[] trIdAfter = lastTrStatus2.getXid().getGlobalTransactionId();
      Assert.assertTrue(Arrays.equals(trIdBefore, trIdAfter));
      return result;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus2.getStatus());
  }

  @Test
  public void test20NotSupportsWithFailOngoingTransaction() {
    final RememberLastCallXAResource lastTrStatus = new RememberLastCallXAResource();
    transactionPropagator.required(() -> {
      enlistResource(lastTrStatus);
      try {
        transactionPropagator.notSupported(() -> {
          throw new NumberFormatException();
        });
        Assert.fail("Should be unreachable code");
      } catch (NumberFormatException e) {
        Assert.assertEquals(Status.STATUS_ACTIVE, getStatus());
      }
      return null;
    });
    Assert.assertEquals(Status.STATUS_COMMITTED, lastTrStatus.getStatus());
  }

  @Test
  public void test21ExceptionSuppression() {
    try {
      transactionPropagator.required(() -> {
        try {
          transactionManager.commit();
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
        throw new NumberFormatException("Test exception");
      });
      Assert.fail("Exception should have been thrown");
    } catch (NumberFormatException e) {
      Assert.assertEquals("Test exception", e.getMessage());
    }
  }

  @Test
  public void test22ExceptionDuringCommit() {
    try {
      transactionPropagator.required(() -> {
        try {
          transactionManager.commit();
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
        return 1;
      });
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals(IllegalStateException.class, e.getClass());
      Assert.assertEquals(0, e.getSuppressed().length);
    }
  }

  @Test
  public void test23ExceptionDuringResume() {
    try {
      transactionPropagator.required(() -> {
        return transactionPropagator.requiresNew(() -> {
          try {
            transactionManager.commit();
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
          return 1;
        });
      });
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals(IllegalStateException.class, e.getClass());
      Assert.assertEquals(0, e.getSuppressed().length);
    }
  }

  @Test
  public void test24SuppressionDuringCommit() {
    try {
      transactionPropagator.required(() -> {
        try {
          transactionManager.commit();
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
        throw new NumberFormatException();
      });
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals(NumberFormatException.class, e.getClass());
      Assert.assertEquals(1, e.getSuppressed().length);
    }
  }

  @Test
  public void test25SuppressionDuringResume() {
    try {
      transactionPropagator.required(() -> {
        return (Integer) transactionPropagator.requiresNew(() -> {
          try {
            transactionManager.commit();
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
          throw new NumberFormatException();
        });
      });
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals(NumberFormatException.class, e.getClass());
      Assert.assertEquals(1, e.getSuppressed().length);
    }
  }

  @Test
  public void test26ThrowingError() {
    try {
      transactionPropagator.required(() -> {
        transactionPropagator.required(() -> {
          throw new Error();
        });
        Assert.assertEquals(Status.STATUS_MARKED_ROLLBACK, getStatus());
      });

    } catch (Error error) {
      // Everything is ok
    }
  }

  @Test
  public void test27ThrowingCheckedExceptionWithUncheckedTrick() {
    try {
      transactionPropagator.required(() -> {
        transactionPropagator.required(() -> {
          Thrower.spit(new IOException());
        });
        Assert.assertEquals(Status.STATUS_MARKED_ROLLBACK, getStatus());
      });

    } catch (Exception e) {
      Assert.assertEquals(RuntimeException.class, e.getClass());
    }
  }
}
