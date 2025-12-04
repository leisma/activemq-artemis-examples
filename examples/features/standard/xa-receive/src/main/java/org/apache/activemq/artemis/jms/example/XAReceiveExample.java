/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.utils.UUIDGenerator;

public class XAReceiveExample {

    public static void main(String[] args) throws Exception {
        InitialContext context = null;
        XAConnection connection1 = null;
        XAConnection connection2 = null;

        try {
            context = new InitialContext();
            Queue queue = (Queue) context.lookup("queue/exampleQueue");
            XAConnectionFactory xaCf = (XAConnectionFactory) context.lookup("XAConnectionFactory");
            ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");

            // 1. step: xa strart and receive with timeout
            System.out.println("Start xa1 receive");
            connection1 = xaCf.createXAConnection();
            connection1.start();
            XASession xaSession1 = connection1.createXASession();
            Session session1 = xaSession1.getSession();
            MessageConsumer consumer1 = session1.createConsumer(queue);
            XAResource xaRes1 = xaSession1.getXAResource();
            Xid xid1 = new DummyXid("xa-test".getBytes(StandardCharsets.US_ASCII), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
            System.out.println("xa start xid1");
            xaRes1.start(xid1, XAResource.TMNOFLAGS);
            System.out.println("Waiting up to 10 minutes for message (terminate pod now!)");
            Message msgBeforeStop = consumer1.receive(120000);

            if (msgBeforeStop == null) {
                System.out.println("No message received");
            } else {
                System.out.println("Received message: " + ((TextMessage) msgBeforeStop).getText());
            }

            // Wait for broker restart
            Thread.sleep(30000);

            // 2. step: new jms connection
            System.out.println("Restart client, create new xa session");
            connection2 = xaCf.createXAConnection();
            connection2.start();
            XASession xaSession2 = connection2.createXASession();
            Session session2 = xaSession2.getSession();
            MessageConsumer consumer2 = session2.createConsumer(queue);
            XAResource xaRes2 = xaSession2.getXAResource();

            // 3. step: xa2 strart and receive with timeout
            Xid xid2 = new DummyXid("xa-test-2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
            System.out.println("xa start xid2");
            xaRes2.start(xid2, XAResource.TMNOFLAGS);

            // 4. step: rollback old transaction within separate thread (wait at the beginning for message receive start)
            Thread rollbackThread = new Thread(() -> {
                try {
                    Thread.sleep(10000);
                    System.out.println("\n[RollbackThread] Rolling back old xid1: " + xid1);

                    try {
                        xaRes2.end(xid1, XAResource.TMFAIL);
                    } catch (XAException e) {
                        e.printStackTrace();
                    }

                    try {
                        xaRes2.rollback(xid1);
                    } catch (XAException e) {
                        e.printStackTrace();
                    }

                    System.out.println("[RollbackThread] Old xa rolled back");
                    Connection connection3 = cf.createConnection();
                    connection3.start();
                    Session session = connection3.createSession();
                    Message messageToSend = session.createTextMessage("New message");
                    session.createProducer(queue).send(messageToSend);
                    session.close();
                    connection3.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            rollbackThread.start();
            System.out.println("start receive message2");
            Message message2 = consumer2.receive(120000); // 1 min

            if (message2 == null) {
                System.out.println("No message received");
            } else {
                System.out.println("Received message: " + ((TextMessage) message2).getText());
            }

            try {
                xaRes2.end(xid2, XAResource.TMSUCCESS);
                System.out.println("End xid: " + xid2);
                xaRes2.prepare(xid2);
                System.out.println("Prepare xid: " + xid2);
                xaRes2.commit(xid2, false);
                System.out.println("Commit xid: " + xid2);
            } catch (Exception e) {
                e.printStackTrace();
                xaRes2.rollback(xid2);
                System.out.println("Rolled back xid: " + xid2);
            }

        } finally {
            if (context != null) context.close();
            if (connection1 != null) connection1.close();
            if (connection2 != null) connection2.close();
        }
    }
}
