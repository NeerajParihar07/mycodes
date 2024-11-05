package dslabs.paxos;


import static dslabs.paxos.PingCheckTimer.PING_CHECK_MILLIS;
import static dslabs.paxos.PingTimer.PING_MILLIS;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] Servers;

  private final AMOApplication<Application> amoApplication;

  // Your code here...
  int slotNumToExecute = 1;
  int cur_SlotNum = 1;
  boolean active;
//  HashMap<Integer, PaxosRequest> Proposals;
//  HashMap<Integer, PaxosRequest> SlotToCommandDecision;
  PaxosRequest [] SlotToCommandDecision = new PaxosRequest[5000];
  PaxosRequest [] Proposals = new PaxosRequest[5000];
  HashMap<PaxosRequest, Integer> CommandToSlotDecision;
  HashMap<PaxosRequest, Integer> CommandToProposals = new HashMap<>();
//  HashMap<Integer, PaxosRequest> acceptedValues;
  PaxosRequest [] acceptedValues = new PaxosRequest[1000];
  private Map<Address, Integer> clientSequences = new HashMap<>();
  private Map<Address, PaxosRequest> pendingRequests = new HashMap<>();
  private Multimap<Integer, Address> phase2Accepted;
  PaxosRequest curRequest;
  HashSet<pValues> Accepted;
  Ballot curBallot;
  HashSet<Address> GotAdoptedBy;
  HashSet<Address> GotAdoptedByCommander;
  HashSet<Address> mostRecentlyPinged;
  HashSet<Address> ActiveServers;
  Address myAddress = null;
  Address clientAddress = null;
  Address[] acceptorNodes;
  Address[] replicaNodes;
  int maxDecisionSlot;
  int maxAcceptedSlot;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.Servers = servers;

    // Your code here...

    maxDecisionSlot = 0;
    maxAcceptedSlot = 0;

    this.amoApplication = new AMOApplication<>(app);

    this.myAddress = address;
    this.acceptorNodes = servers;
    this.replicaNodes = servers;

    active = false;
    curRequest = null;

//    Proposals = new HashMap<>();
    CommandToSlotDecision = new HashMap<>();
//    acceptedValues = new HashMap<>();

    Accepted = new HashSet<>();

    GotAdoptedBy = new HashSet<Address>();
    GotAdoptedByCommander = new HashSet<Address>();
    mostRecentlyPinged = new HashSet<>();
    ActiveServers = new HashSet<>();
    
    
    phase2Accepted = HashMultimap.create();
  }

  @Override
  public void init() {
    // Your code here...

    this.curBallot = new Ballot(1, this.Servers[0]);
    this.active = false;
//    Proposals = new HashMap<>();
    ActiveServers.add(this.myAddress);

    if(Objects.equals(Servers[0], myAddress)){
      send(new Ping(slotNumToExecute),Servers[0]);
      set(new PingTimer(), PING_MILLIS);
      set(new PingCheckTimer(), PING_CHECK_MILLIS);
    }

    callScout(this.myAddress, acceptorNodes);

  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    if (SlotToCommandDecision[logSlotNum] != null) return PaxosLogSlotStatus.CHOSEN;
    else if (acceptedValues[logSlotNum] != null) {
      return PaxosLogSlotStatus.ACCEPTED;
    } else {
      return PaxosLogSlotStatus.EMPTY;
    }

    //    return null;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    if (SlotToCommandDecision[logSlotNum] != null)
      return SlotToCommandDecision[logSlotNum].operation.command();
    else if (acceptedValues[logSlotNum] != null)
      return acceptedValues[logSlotNum].operation.command();
    return null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    return 1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    if (maxDecisionSlot == 0 && maxAcceptedSlot == 0) {
      return 0;
    } else {
      return Math.max(maxDecisionSlot, maxAcceptedSlot);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    Integer lastSeq = clientSequences.get(sender);
    if (lastSeq != null && m.sequenceNum <= lastSeq) {
      return;
    }

    pendingRequests.put(sender, m);
    propose(m);
  }

  void propose(PaxosRequest m) {
    if (CommandToSlotDecision.containsKey(m) || CommandToProposals.containsKey(m)) {
      return;
    }

    int slot = slotNumToExecute;
    //Proposals.containsKey(slot) ||
    while (SlotToCommandDecision[slot] != null) {
      slot++;
    }

    Proposals[slot] = m;
    if(Objects.equals(this.myAddress, Servers[0])) {
      CallProposeMessage(slot, m);
    }
//    else send(new ProposeMessage(m.clientAddress,slot,m), Servers[0]);
  }

  void handleProposeMessage(ProposeMessage m, Address sender) {
    if (Proposals[m.slot_num] == null || (Proposals[m.slot_num] != null && Objects.equals(Proposals[m.slot_num], m.operation))) {
        if (active) {
          Proposals[m.slot_num] = m.operation;
          callCommander(acceptorNodes, new pValues(curBallot, m.slot_num, m.operation));
        }
    }
  }

  void CallProposeMessage(int slot_num, PaxosRequest m) {
    if (Proposals[slot_num] == null || (Proposals[slot_num] != null && Objects.equals(Proposals[slot_num], m))) {
      if (active) {
        Proposals[slot_num] = m;
        callCommander(acceptorNodes, new pValues(curBallot, slot_num, m));
      }
    }
  }

  void handleP2aMessage(P2aMessage m, Address sender) {
    if (compare(m.pValue.ballot, this.curBallot) >= 0) {
      this.curBallot = m.pValue.ballot;
      maxAcceptedSlot = Math.max(maxAcceptedSlot, m.pValue.slot_num);
      acceptedValues[m.pValue.slot_num] = m.pValue.operation;
    }

    if(Objects.equals(myAddress, Servers[0]))handleP2bMessage(new P2bMessage( this.myAddress, new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)), m.leaderAddress);
    else send(new P2bMessage( this.myAddress, new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)), m.leaderAddress);

    if(NotMe(m.leaderAddress))send(
        new P2bMessage(
            this.myAddress, new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)),
        m.leaderAddress);
  }


  void perform(PaxosRequest m) {
    for (int i = 1; i < slotNumToExecute; i++) {
      if (SlotToCommandDecision[i] != null && Objects.equals(SlotToCommandDecision[i], m)) {
        slotNumToExecute++;
        return;
      }
    }
    AMOResult result = this.amoApplication.execute(m.operation);
    clientSequences.put(m.clientAddress, m.sequenceNum);
    if (pendingRequests.containsKey(m.clientAddress)) {
      send(new PaxosReply(m.sequenceNum, result), m.clientAddress);
      pendingRequests.remove(m.clientAddress);
    }
    slotNumToExecute++;
  }

  // Your code here...
  void callScout(Address sender, Address[] acceptors) {
    GotAdoptedBy.clear();
    for (int i = 0; i < acceptors.length; i++) {
//      Address address = acceptors[i];
      GotAdoptedBy.add(acceptors[i]);
//      if(Objects.equals(address, myAddress))handleP1aMessage(new P1aMessage(this.myAddress, curBallot), address);
//      else send(new P1aMessage(this.myAddress, curBallot), address);
    }
    Broadcast(new P1aMessage(this.myAddress, curBallot));
    handleP1aMessage(new P1aMessage(this.myAddress, curBallot), myAddress);
  }

  void  commandExecution()
  {
    ArrayList<Integer> toPropose = new ArrayList<>();
    while (SlotToCommandDecision[slotNumToExecute] != null) {
      if (Proposals[slotNumToExecute] != null
          && !Objects.equals(
          Proposals[slotNumToExecute], SlotToCommandDecision[slotNumToExecute])) {
        toPropose.add(slotNumToExecute);
      }
      perform(SlotToCommandDecision[slotNumToExecute]);
    }

    for(Integer a : toPropose)propose(Proposals[a]);
  }

  void handleP1aMessage(P1aMessage m, Address sender) {
    if (compare(m.ballot, this.curBallot) > 0) this.curBallot = m.ballot;
    Broadcast(new P1bMessage(this.myAddress, curBallot, Accepted));
//    for(Address address: Servers)
//    {
//      if(Objects.equals(address, myAddress))handleP1bMessage(new P1bMessage(this.myAddress, curBallot, Accepted), address);
//      else send(new P1bMessage(this.myAddress, curBallot, Accepted), address);
////      send(new P1bMessage(this.myAddress, curBallot, Accepted), address);
//    }
    handleP1bMessage(new P1bMessage(this.myAddress, curBallot, Accepted), myAddress);
  }

  public void Broadcast(Message m)
  {
    ArrayList<Address> senders = new ArrayList<>(Arrays.asList(Servers));
    senders.remove(myAddress);
    broadcast(m, senders.toArray(new Address[senders.size()]));
  }

  void handleP1bMessage(P1bMessage m, Address sender) {
    if (!Objects.equals(m.ballot, curBallot)) {
      send(new PreemptedMessage(myAddress, m.ballot), Servers[0]);
      return;
    }

    GotAdoptedBy.remove(sender);
    Accepted.addAll(m.accepted);

    int majority = Servers.length / 2;
    if ((Servers.length - GotAdoptedBy.size()) > majority) {
      if(Objects.equals(myAddress, Servers[0]))callAdoptedMessage(new AdoptedMessage(myAddress, curBallot, Accepted));
    }
  }

  private void callAdoptedMessage(AdoptedMessage msg) {
    if (curBallot.equals(msg.ballot)) {
      Map<Integer, pValues> pmax = new HashMap<>();
      for (pValues pv : msg.accepted) {
        int sn = pv.slot_num;
        Ballot bn = pv.ballot;
        if (!pmax.containsKey(sn) || compare(pmax.get(sn).ballot, bn) < 0) {
          pmax.put(sn, pv);
          Proposals[sn] = pv.operation;
        }
      }
      active = true;
      for(int j = 1;;j++)
      {
        if(Proposals[j] == null)break;
        PaxosRequest cmd = Proposals[j];
        callCommander(acceptorNodes, new pValues(msg.ballot, j, cmd));
      }
    }
  }

  void callCommander(Address[] acceptorNodes, pValues cur) {
    GotAdoptedByCommander.clear();
    for (int i = 0; i < acceptorNodes.length; i++) {
      Address address = acceptorNodes[i];
      GotAdoptedByCommander.add(address);
//      if(Objects.equals(address, myAddress))send(new P2aMessage(this.myAddress, cur), acceptorNodes[i]);
//      else send(new P2aMessage(this.myAddress, cur), acceptorNodes[i]);
//      send(new P2aMessage(this.myAddress, cur), acceptorNodes[i]);
    }
    Broadcast(new P2aMessage(this.myAddress, cur));
    send(new P2aMessage(this.myAddress, cur), myAddress);
  }

  void handleP2bMessage(P2bMessage m, Address sender) {
    if (!Objects.equals(m.pValue.ballot, curBallot)) {
      send(new PreemptedMessage(myAddress, curBallot), Servers[0]);
      return;
    }

    GotAdoptedByCommander.remove(sender);

    if ((Servers.length - GotAdoptedBy.size()) > Servers.length / 2) {
//      if(SlotToCommandDecision[m.pValue.slot_num] != null || CommandToSlotDecision.containsKey(m.pValue.operation))return;
      if(SlotToCommandDecision[m.pValue.slot_num] != null)return;
      CommandToSlotDecision.put(m.pValue.operation, m.pValue.slot_num);
      if(SlotToCommandDecision[m.pValue.slot_num] == null)SlotToCommandDecision[m.pValue.slot_num] = m.pValue.operation;
      if(Objects.equals(myAddress, Servers[0]))
      {
        if(SlotToCommandDecision[m.pValue.slot_num] == null)SlotToCommandDecision[m.pValue.slot_num] = m.pValue.operation;
//        for (Address replica : replicaNodes) {
//          if (!NotMe(replica)) continue;
//          if(Objects.equals(replica, Servers[0]))handleDecisionMessage(new DecisionMessage(replica, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]), replica);
//          else send(new DecisionMessage(replica, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]), replica);
//        }
        Broadcast(new DecisionMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]));
        handleDecisionMessage(new DecisionMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]), myAddress);
      }
    }
  }

  void handleDecisionMessage(DecisionMessage m, Address sender) {
    maxDecisionSlot = Math.max(maxDecisionSlot, m.slot_num);
    SlotToCommandDecision[m.slot_num]  = m.operation;
    CommandToSlotDecision.put(m.operation, m.slot_num);
    commandExecution();
  }

  private void handlePreemptedMessage(PreemptedMessage msg, Address sender) {
    if (compare(msg.ballot, this.curBallot) > 0) {
      active = false;
      curBallot = new Ballot(msg.ballot.ballot_num + 1, this.myAddress);
      callScout(myAddress, acceptorNodes);
    }
  }

  int compare(Ballot x, Ballot y) {
    if (x.ballot_num() != y.ballot_num()) return (x.ballot_num > y.ballot_num ? 1 : -1);
    return x.leader.compareTo(y.leader);
  }

  private void handlePing(Ping m, Address sender) {
    CommandToProposals.clear();
    mostRecentlyPinged.add(sender);
    send(new AckMessage(myAddress, slotNumToExecute), this.Servers[0]);
  }


  private void onPingCheckTimer(PingCheckTimer t) {
    mostRecentlyPinged.add(myAddress); // Include self
    ActiveServers.clear();
    ActiveServers.addAll(mostRecentlyPinged);
    mostRecentlyPinged.clear();

    mostRecentlyPinged.clear();
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  private void onPingTimer(PingTimer t) {
    if(this.myAddress != Servers[0])return;
//    for(Address server: Servers)if(NotMe(this.Servers[0]))send(new Ping(slotNumToExecute), server);
    Broadcast(new Ping(slotNumToExecute));
    set(new PingTimer(), PING_MILLIS);
  }

  private void handleAckMessage(AckMessage msg, Address sender)
  {
    mostRecentlyPinged.add(sender);
    if(msg.slot < slotNumToExecute)
    {
      PaxosRequest[] missingDecisions = new PaxosRequest[slotNumToExecute - msg.slot];
      System.arraycopy(SlotToCommandDecision, msg.slot,
          missingDecisions, 0, missingDecisions.length);
      send(new decisionTransfer(msg.slot, missingDecisions), sender);
    }
  }

  private void handledecisionTransfer(decisionTransfer msg, Address sender)
  {
    if(msg.slot > slotNumToExecute)
    {
      // Only update slots that we haven't executed yet
      System.arraycopy(msg.decision_list, 0,
          SlotToCommandDecision, msg.slot,
          msg.decision_list.length);

      // Update any command-to-slot mappings for the new decisions
      for (int i = 0; i < msg.decision_list.length; i++) {
        PaxosRequest decision = msg.decision_list[i];
        if (decision != null) {
          CommandToSlotDecision.put(decision, msg.slot + i);
        }
      }

      // Execute any newly available commands
      commandExecution();
    }
  }
  
  private boolean NotMe(Address x)
  {
    return true;
  }
}
