package dslabs.paxos;


import static dslabs.paxos.PingCheckTimer.PING_CHECK_MILLIS;
import static dslabs.paxos.PingTimer.PING_MILLIS;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
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
  PaxosRequest [] SlotToCommandDecision = new PaxosRequest[1000];
  PaxosRequest [] Proposals = new PaxosRequest[1000];
  HashMap<PaxosRequest, Integer> CommandToSlotDecision;
  HashMap<PaxosRequest, Integer> CommandToProposals = new HashMap<>();
//  HashMap<Integer, PaxosRequest> acceptedValues;
  PaxosRequest [] acceptedValues = new PaxosRequest[1000];
  private Map<Address, Integer> clientSequences = new HashMap<>();
  private Map<Address, PaxosRequest> pendingRequests = new HashMap<>();
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
//      send(new ProposeMessage(m.clientAddress, slot, m), this.Servers[0]);
      CallProposeMessage(slot, m);
    }
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
    if(NotMe(m.leaderAddress))send(
        new P2bMessage(
            this.myAddress, new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)),
        m.leaderAddress);
//    if(Objects.equals(myAddress, Servers[0]))
//    {
//      callP2bMessage(new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation) ,sender);
//    }
  }

//  void callP2bMessage(pValues m, Address sender) {
//    if (!Objects.equals(m.ballot, curBallot)) {
//      send(new PreemptedMessage(myAddress, curBallot), Servers[0]);
//      return;
//    }
//
//    GotAdoptedByCommander.remove(sender);
//
//    if ((Servers.length - GotAdoptedBy.size()) > Servers.length / 2) {
//      if(SlotToCommandDecision[m.slot_num] != null || CommandToSlotDecision.containsKey(m.operation))return;
//      CommandToSlotDecision.put(m.operation, m.slot_num);
//      if(SlotToCommandDecision[m.slot_num] == null)SlotToCommandDecision[m.slot_num] = m.operation;
//      CommanderResponseMessage res = new CommanderResponseMessage(myAddress, m.slot_num, SlotToCommandDecision[m.slot_num]);
//      send(res, Servers[0]);
//    }
//  }


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
      Address address = acceptors[i];
      GotAdoptedBy.add(acceptors[i]);
      if(NotMe(address))send(new P1aMessage(this.myAddress, curBallot), address);
    }
  }

  void  commandExecution()
  {
    while (SlotToCommandDecision[slotNumToExecute] != null) {
      if (Proposals[slotNumToExecute] != null
          && !Objects.equals(
          Proposals[slotNumToExecute], SlotToCommandDecision[slotNumToExecute])) {
        propose(Proposals[slotNumToExecute]);
      }
      perform(SlotToCommandDecision[slotNumToExecute]);
    }
  }

  void handleP1aMessage(P1aMessage m, Address sender) {
    if (compare(m.ballot, this.curBallot) > 0) this.curBallot = m.ballot;
    for(Address address: Servers)send(new P1bMessage(this.myAddress, curBallot, Accepted), address);
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
      send(new AdoptedMessage(myAddress, curBallot, Accepted), Servers[0]);
    }
  }

  private void handleAdoptedMessage(AdoptedMessage msg, Address sender) {
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
//      for (Map.Entry<Integer, PaxosRequest> entry : Proposals.entrySet()) {
//        int sn = entry.getKey();
//        PaxosRequest cmd = entry.getValue();
//        callCommander(acceptorNodes, new pValues(msg.ballot, sn, cmd));
//      }
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
      send(new P2aMessage(this.myAddress, cur), acceptorNodes[i]);
    }
  }

  void handleP2bMessage(P2bMessage m, Address sender) {
    if (!Objects.equals(m.pValue.ballot, curBallot)) {
      send(new PreemptedMessage(myAddress, curBallot), Servers[0]);
      return;
    }

    GotAdoptedByCommander.remove(sender);

    if ((Servers.length - GotAdoptedBy.size()) > Servers.length / 2) {
      if(SlotToCommandDecision[m.pValue.slot_num] != null || CommandToSlotDecision.containsKey(m.pValue.operation))return;
      CommandToSlotDecision.put(m.pValue.operation, m.pValue.slot_num);
      if(SlotToCommandDecision[m.pValue.slot_num] == null)SlotToCommandDecision[m.pValue.slot_num] = m.pValue.operation;
      if(Objects.equals(myAddress, Servers[0]))
      {
        if(SlotToCommandDecision[m.pValue.slot_num] == null)SlotToCommandDecision[m.pValue.slot_num] = m.pValue.operation;
        for (Address replica : replicaNodes) {
          if (!NotMe(replica)) continue;
          send(new DecisionMessage(replica, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]), replica);
        }
      }
//      CommanderResponseMessage res = new CommanderResponseMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision[m.pValue.slot_num]);
//      send(res, Servers[0]);
    }
  }

  void handleCommanderResponseMessage(CommanderResponseMessage m, Address sender)
  {
    if(SlotToCommandDecision[m.slot_num] == null)SlotToCommandDecision[m.slot_num] = m.operation;
    for (Address replica : replicaNodes) {
      if (!NotMe(replica)) continue;
      send(new DecisionMessage(replica, m.slot_num, SlotToCommandDecision[m.slot_num]), replica);
    }
  }


  void handleDecisionMessage(DecisionMessage m, Address sender) {
//    System.out.println(" Slot NUM TO EXECUTE " + slotNumToExecute + " Got the decision Message "  + m);
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
//    System.out.println("@@@@@@@@  --> " + slotNumToExecute  +  " new " + m +  " old list " +  SlotToCommandDecision);
//    if(slotNumToExecute < m.CurrentSlotToExecute)
//    {
//      SlotToCommandDecision = m.decision_list;
//      commandExecution();
//    }
    CommandToProposals.clear();
    mostRecentlyPinged.add(sender);
    send(new AckMessage(myAddress, slotNumToExecute), this.Servers[0]);
  }


  private void onPingCheckTimer(PingCheckTimer t) {
    mostRecentlyPinged.add(myAddress); // Include self
    ActiveServers.clear();
    ActiveServers.addAll(mostRecentlyPinged);
    mostRecentlyPinged.clear();
//    int id = 0;
//    for(int i = Servers.length-1 ; i >= 0; i--)if(ActiveServers.contains(Servers[i]))id = i;
//    Address temp = Servers[0];
//    Servers[0] = Servers[id];
//    Servers[id] = temp;

    mostRecentlyPinged.clear();
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  private void onPingTimer(PingTimer t) {
    if(this.myAddress != Servers[0])return;
    for(Address server: Servers)if(NotMe(this.Servers[0]))send(new Ping(slotNumToExecute), server);
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
      SlotToCommandDecision = msg.decision_list;
      commandExecution();
    }
  }
  
  private boolean NotMe(Address x)
  {
    return true;
//    return !Objects.equals(this.myAddress, x);
  }
}
