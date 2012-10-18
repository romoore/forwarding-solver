/*
 * Owl Platform Forwarding Solver
 * Copyright (C) 2012 Robert Moore and the Owl Platform
 * Copyright (C) 2011 Rutgers University and Robert Moore
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *  
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.owlplatform.solver.forwarding;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlplatform.common.SampleMessage;
import com.owlplatform.sensor.SensorAggregatorInterface;
import com.owlplatform.sensor.listeners.ConnectionListener;
import com.owlplatform.solver.SolverAggregatorInterface;
import com.owlplatform.solver.listeners.SampleListener;
import com.owlplatform.solver.protocol.messages.SubscriptionMessage;

public class ForwardingSolver implements ConnectionListener,
    com.owlplatform.solver.listeners.ConnectionListener, SampleListener {

  private static final Logger log = LoggerFactory
      .getLogger(ForwardingSolver.class);

  protected final ConcurrentLinkedQueue<SolverAggregatorInterface> inputAggregators = new ConcurrentLinkedQueue<SolverAggregatorInterface>();

  protected final ConcurrentLinkedQueue<SensorAggregatorInterface> outputAggregators = new ConcurrentLinkedQueue<SensorAggregatorInterface>();

  protected final LinkedBlockingQueue<SampleMessage> sampleQueue = new LinkedBlockingQueue<SampleMessage>();

  protected final SampleWorker[] workers;
  
  protected static int samplesForwarded = 0;
  
  Timer statsTimer = new Timer("Stats Timer");

  public ForwardingSolver(final int numOutputs) {
    this.workers = new SampleWorker[numOutputs];

    for (int i = 0; i < numOutputs; ++i) {
      this.workers[i] = new SampleWorker(this, this.sampleQueue);
      this.workers[i].start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        ForwardingSolver.this.shutdown();
      }
    });
    
    this.statsTimer.schedule(new TimerTask() {
      
      @Override
      public void run() {
        int samples = ForwardingSolver.samplesForwarded;
        ForwardingSolver.samplesForwarded = 0;
        
        log.info("Forwarded {} samples in the last 10 seconds.", samples);
      }
    }, 10000, 10000);
  }

  @Override
  public void connectionEnded(SensorAggregatorInterface aggregator) {
    aggregator.removeConnectionListener(this);
    this.outputAggregators.remove(aggregator);
    log.info("Connection to {} ended. No longer forwarding samples.",
        aggregator);
  }

  @Override
  public void connectionEstablished(SensorAggregatorInterface aggregator) {
    log.info("Connecting to {} to send samples.", aggregator);
  }

  @Override
  public void connectionInterrupted(SensorAggregatorInterface aggregator) {
    this.outputAggregators.remove(aggregator);
    log.warn("Connection to {} was interrupted. Samples will not be sent until the connection resumes.");

  }

  @Override
  public void readyForSamples(SensorAggregatorInterface aggregator) {
    this.outputAggregators.add(aggregator);
    log.info("Ready to send samples to {}", aggregator);
  }

  @Override
  public void connectionEnded(SolverAggregatorInterface aggregator) {
    aggregator.removeConnectionListener(this);
    aggregator.removeSampleListener(this);
    this.inputAggregators.remove(aggregator);
    log.info("Connection to {} ended.  No longer receiving samples.",
        aggregator);
  }

  @Override
  public void connectionEstablished(SolverAggregatorInterface aggregator) {
    this.inputAggregators.add(aggregator);
    log.info("Connecting to {} to receive samples.", aggregator);
  }

  @Override
  public void connectionInterrupted(SolverAggregatorInterface aggregator) {
    log.info(
        "Connection to {} was interrupted.  Samples will not be sent until the connection resumes.",
        aggregator);
  }

  @Override
  public void sampleReceived(final SolverAggregatorInterface aggregator,
      final SampleMessage sample) {

    if (!this.sampleQueue.offer(sample)) {
      log.warn("Queue full, dropping a sample.");
    }
  }

  private static final class SampleWorker extends Thread {
    private static final long POLL_PERIOD = 50;
    private static final TimeUnit POLL_UNIT = TimeUnit.MILLISECONDS;
    private final LinkedBlockingQueue<SampleMessage> sampleQueue;
    private final ForwardingSolver solver;

    private boolean keepRunning = true;

    public SampleWorker(ForwardingSolver solver,
        LinkedBlockingQueue<SampleMessage> sampleQueue) {
      this.sampleQueue = sampleQueue;
      this.solver = solver;
    }

    public void run() {
      while (this.keepRunning) {
        try {
          SampleMessage sample = this.sampleQueue.poll(POLL_PERIOD, POLL_UNIT);
          if(sample == null){
            continue;
          }
          
          this.solver.processSample(sample);

        } catch (InterruptedException ie) {
          continue;
        }
      }
    }

    public void shutdown() {
      this.keepRunning = false;
    }

  }

  protected void processSample(final SampleMessage sample) {

    ++this.samplesForwarded;
    
    for (SensorAggregatorInterface outAgg : this.outputAggregators) {

      outAgg.sendSample(sample);

    }
  }

  public void addOutput(final String hostname, final Integer port,
      final int sampleBufferSize) {
    SensorAggregatorInterface newAgg = new SensorAggregatorInterface();
    newAgg.setHost(hostname);
    newAgg.setPort(port.intValue());
    newAgg.addConnectionListener(this);
    newAgg.setStayConnected(true);
    if (sampleBufferSize > 0) {
      log.info("Buffering {} samples for {}", sampleBufferSize,this);
      newAgg.setMaxOutstandingSamples(sampleBufferSize);
    }
    // TODO: Make sure aggregators get reconnected
    if (newAgg.doConnectionSetup()) {
      log.info("Connection for outgoing samples succeeded to {}", newAgg);
    } else {
      log.error("Failed connection for outgoing samples to {}", newAgg);
    }
  }

  public void addInput(final String hostname, final Integer port) {
    SolverAggregatorInterface newAgg = new SolverAggregatorInterface();
    newAgg.setHost(hostname);
    newAgg.setPort(port.intValue());
    newAgg.addConnectionListener(this);
    newAgg.addSampleListener(this);
    newAgg.setStayConnected(true);
    if (newAgg.doConnectionSetup()) {
      log.info("Connection for incoming samples succeeded to {}", newAgg);
    } else {
      log.warn("Connection for incoming samples failed to {}", newAgg);
    }
  }

  public void removeInput(final SolverAggregatorInterface aggregator) {
    this.inputAggregators.remove(aggregator);
    aggregator.doConnectionTearDown();
  }

  public void removeOutput(final SensorAggregatorInterface aggregator) {
    this.outputAggregators.remove(aggregator);
    aggregator.doConnectionTearDown();
  }

  public static void main(String[] args) {
    if(args.length == 0){
      System.out.println("Parameters: [-i fromAggregator port]+ [-o toAggregator port]+");
      return;
    }
    
    int bufferSize = -1;

    String buffString = System.getProperty("buffer");
    if (buffString != null) {
      try {
        bufferSize = Integer.parseInt(buffString);
      } catch (NumberFormatException nfe) {
        System.err
            .println("The value for \"buffer\" must be an integer value.");
      }
    }

    // Expects --input host port --output host port ...
    // Alternatives -i host port -o host port
    // Basically flag followed by host, port #.
    // -i or --input for receiving samples
    // -o or --output for sending/forwarding samples
    List<String> inputHosts = new ArrayList<String>();
    List<Integer> inputPorts = new ArrayList<Integer>();

    List<String> outputHosts = new ArrayList<String>();
    List<Integer> outputPorts = new ArrayList<Integer>();

    for (int i = 0; i < args.length; ++i) {
      // Process input host and port
      if ("-i".equals(args[i]) || "--input".equals(args[i])) {
        String host = args[++i];
        Integer port = Integer.valueOf(args[++i]);
        inputHosts.add(host);
        inputPorts.add(port);
      } else if ("-o".equals(args[i]) || "--output".equals(args[i])) {
        String host = args[++i];
        Integer port = Integer.valueOf(args[++i]);
        outputHosts.add(host);
        outputPorts.add(port);
      } else {
        log.warn("Unknown option {}. Skipping...", args[i]);
      }
    }

    ForwardingSolver solver = new ForwardingSolver(outputHosts.size());
    
    for (int i = 0; i < outputHosts.size(); ++i) {
      solver.addOutput(outputHosts.get(i), outputPorts.get(i), bufferSize);
    }
    for (int i = 0; i < inputHosts.size(); ++i) {
      solver.addInput(inputHosts.get(i), inputPorts.get(i));
    }
    
  }

  @Override
  public void subscriptionReceived(SolverAggregatorInterface aggregator,
      SubscriptionMessage response) {
    // TODO Auto-generated method stub

  }

  public void shutdown() {
    for (SampleWorker worker : this.workers) {
      worker.shutdown();
    }

    for (SampleWorker worker : this.workers) {
      try {
        worker.join(100);
      } catch (InterruptedException ie) {
        log.warn("A worker did not terminate gracefully.");
      }
    }
  }
}
