package dslabs.paxos;

import static dslabs.paxos.PingCheckTimer.PING_CHECK_MILLIS;
import static dslabs.paxos.PingTimer.PING_MILLIS;
import static java.lang.Thread.sleep;

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
import org.checkerframework.checker.units.qual.A;

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
  HashMap<Integer, PaxosRequest> SlotToCommandDecision;
//  PaxosRequest [] SlotToCommandDecision = new PaxosRequest[10000];
  PaxosRequest [] Proposals = new PaxosRequest[10000];
  HashMap<PaxosRequest, Integer> CommandToSlotDecision;
  HashMap<PaxosRequest, Integer> CommandToProposals = new HashMap<>();
//  HashMap<Integer, PaxosRequest> acceptedValues;
  PaxosRequest [] acceptedValues = new PaxosRequest[10000];
  private Map<Address, Integer> clientSequences = new HashMap<>();
  private Map<Address, PaxosRequest> pendingRequests = new HashMap<>();
  private Multimap<pValues, Address> phase2Accepted;
  PaxosRequest curRequest;
  HashSet<pValues> Accepted;
  Ballot curBallotAcceptor;
  Ballot curBallotLeader;
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
  int max_slot_to_execute;
  Address LeaderAddress;
  Address newLeader;
  Ballot maxBallot = null;
  boolean goToGo = false;

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
    CommandToSlotDecision = new HashMap<>();
    SlotToCommandDecision = new HashMap<>();
    LeaderAddress = Servers[0];
    this.curBallotAcceptor = new Ballot(0, myAddress);
    this.curBallotLeader = new Ballot(1, LeaderAddress);
    maxBallot = new Ballot(curBallotLeader.ballot_num, curBallotLeader.leader);
    this.active = false;
    ActiveServers.add(this.myAddress);
    newLeader = LeaderAddress;

    if(Objects.equals(LeaderAddress, myAddress)){
      Broadcast(new Ping(slotNumToExecute, curBallotLeader), Servers);
    }
    set(new PingTimer(), PING_MILLIS);
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
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
    if (SlotToCommandDecision.containsKey(logSlotNum)) return PaxosLogSlotStatus.CHOSEN;
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
    if (SlotToCommandDecision.containsKey(logSlotNum))
      return SlotToCommandDecision.get(logSlotNum).operation.command();
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
//    System.out.println("Me " + myAddress + " leader " + LeaderAddress +  " ballot - " + curBallotLeader);

    pendingRequests.put(sender, m);
    propose(m);
  }

  void propose(PaxosRequest m) {
    if (CommandToSlotDecision.containsKey(m) || CommandToProposals.containsKey(m) || !Objects.equals(myAddress, LeaderAddress)) {
      return;
    }

    int slot = slotNumToExecute;
    while (SlotToCommandDecision.containsKey(slot)) {
      slot++;
    }

    Proposals[slot] = m;
    CallProposeMessage(slot, m);
  }

  void handleProposeMessage(ProposeMessage m, Address sender) {
    if (Proposals[m.slot_num] == null || (Proposals[m.slot_num] != null && Objects.equals(Proposals[m.slot_num], m.operation))) {
        if (active) {
          Proposals[m.slot_num] = m.operation;
          callCommander(acceptorNodes, new pValues(curBallotLeader, m.slot_num, m.operation));
        }
    }
  }

  void CallProposeMessage(int slot_num, PaxosRequest m) {
    if (Proposals[slot_num] == null || (Proposals[slot_num] != null && Objects.equals(Proposals[slot_num], m))) {
      if (active) {
        Proposals[slot_num] = m;
        callCommander(acceptorNodes, new pValues(curBallotLeader, slot_num, m));
      }
    }
  }

  void handleP2aMessage(P2aMessage m, Address sender) {
    if(SlotToCommandDecision.containsKey(m.pValue.slot_num) && !Objects.equals(SlotToCommandDecision.get(m.pValue.slot_num), m.pValue.operation))return;
    if(acceptedValues[m.pValue.slot_num]!=null)return;
    if (compare(m.pValue.ballot, this.curBallotAcceptor) >= 0) {
      this.curBallotAcceptor = new Ballot(m.pValue.ballot.ballot_num, m.pValue.ballot().leader);
      maxAcceptedSlot = Math.max(maxAcceptedSlot, m.pValue.slot_num);
      acceptedValues[m.pValue.slot_num] = m.pValue.operation;
    }

    if(Objects.equals(myAddress, LeaderAddress))handleP2bMessage(new P2bMessage( this.myAddress, new pValues(this.curBallotAcceptor, m.pValue.slot_num, m.pValue.operation)), m.leaderAddress);
    else send(new P2bMessage( this.myAddress, new pValues(this.curBallotAcceptor, m.pValue.slot_num, m.pValue.operation)), m.leaderAddress);

    if(NotMe(m.leaderAddress))send(
        new P2bMessage(
            this.myAddress, new pValues(this.curBallotAcceptor, m.pValue.slot_num, m.pValue.operation)),
        m.leaderAddress);
  }


  void perform(PaxosRequest m) {
    for (int i = 1; i < slotNumToExecute; i++) {
      if (SlotToCommandDecision.containsKey(i) && Objects.equals(SlotToCommandDecision.get(i), m)) {
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
    for (int i = 0; i < acceptors.length; i++)GotAdoptedBy.add(acceptors[i]);
    Broadcast(new P1aMessage(this.myAddress, curBallotLeader), Servers);
    handleP1aMessage(new P1aMessage(this.myAddress, curBallotLeader), myAddress);
  }

  void  commandExecution()
  {
    ArrayList<Integer> toPropose = new ArrayList<>();
    while (SlotToCommandDecision.containsKey(slotNumToExecute)) {
      if (Proposals[slotNumToExecute] != null
          && !Objects.equals(
          Proposals[slotNumToExecute], SlotToCommandDecision.get(slotNumToExecute))) {
        toPropose.add(slotNumToExecute);
      }
      perform(SlotToCommandDecision.get(slotNumToExecute));
    }

    for(Integer a : toPropose)propose(Proposals[a]);
  }

  void handleP1aMessage(P1aMessage m, Address sender) {
    if (compare(m.ballot, this.curBallotAcceptor) > 0) this.curBallotAcceptor = new Ballot(m.ballot.ballot_num, m.ballot().leader);
    Broadcast(new P1bMessage(this.myAddress, curBallotAcceptor, Accepted), acceptorNodes);
    handleP1bMessage(new P1bMessage(this.myAddress, curBallotAcceptor, Accepted), myAddress);
  }

  public void Broadcast(Message m, Address [] Nodes)
  {
    ArrayList<Address> senders = new ArrayList<>(Arrays.asList(Nodes));
    senders.remove(myAddress);
    broadcast(m, senders.toArray(new Address[senders.size()]));
  }

  void handleP1bMessage(P1bMessage m, Address sender) {
    if (!Objects.equals(m.ballot, curBallotLeader)) {
      handlePreemptedMessage(new PreemptedMessage(myAddress, m.ballot), LeaderAddress);
      return;
    }

    GotAdoptedBy.remove(sender);
    Accepted.addAll(m.accepted);

    int majority = Servers.length / 2;
    if ((Servers.length - GotAdoptedBy.size()) > majority) {
      if(Objects.equals(myAddress, LeaderAddress))callAdoptedMessage(new AdoptedMessage(myAddress, curBallotLeader, Accepted));
    }
  }

  private void callAdoptedMessage(AdoptedMessage msg) {
    if (curBallotLeader.equals(msg.ballot)) {
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
    }
    phase2Accepted.clear();
    Broadcast(new P2aMessage(this.myAddress, cur), acceptorNodes);
    handleP2aMessage(new P2aMessage(this.myAddress, cur), myAddress);
  }

  void handleP2bMessage(P2bMessage m, Address sender) {
    if (!Objects.equals(m.pValue.ballot, curBallotLeader)) {
      handlePreemptedMessage(new PreemptedMessage(myAddress, m.pValue.ballot), LeaderAddress);
      return;
    }

    phase2Accepted.put(m.pValue, sender);

    if (phase2Accepted.get(m.pValue).size() > Servers.length / 2) {

//      System.out.println("this value got accepted by majority " + m.pValue + " at " + myAddress);

      if(SlotToCommandDecision.containsKey(m.pValue.slot_num))return;
      CommandToSlotDecision.put(m.pValue.operation, m.pValue.slot_num);
      if(!SlotToCommandDecision.containsKey(m.pValue.slot_num))SlotToCommandDecision.put(m.pValue.slot_num,m.pValue.operation);
      if(Objects.equals(myAddress, LeaderAddress))
      {
        if(SlotToCommandDecision.containsKey(m.pValue.slot_num))SlotToCommandDecision.put(m.pValue.slot_num,m.pValue.operation);
        handleDecisionMessage(new DecisionMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision.get(m.pValue.slot_num)), myAddress);
        Broadcast(new DecisionMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision.get(m.pValue.slot_num)), Servers);
      }
    }
  }

  void handleDecisionMessage(DecisionMessage m, Address sender) {
    maxDecisionSlot = Math.max(maxDecisionSlot, m.slot_num);
    SlotToCommandDecision.put(m.slot_num, m.operation);
    CommandToSlotDecision.put(m.operation, m.slot_num);
    commandExecution();
  }

  private void handlePreemptedMessage(PreemptedMessage msg, Address sender) {
    if (compare(msg.ballot, this.curBallotLeader) > 0) {
      active = false;
      if(msg.ballot.leader.compareTo(myAddress) == 0){
        curBallotLeader = new Ballot(msg.ballot.ballot_num + 1, LeaderAddress);
//      System.out.println("Are we changing it over here ?? " + myAddress + " " + curBallotLeader + " " + LeaderAddress);
        callScout(myAddress, acceptorNodes);
      }
    }
  }

  int compare(Ballot x, Ballot y) {
    if (x.ballot_num() != y.ballot_num()) return (x.ballot_num > y.ballot_num ? 1 : -1);
    return x.leader.compareTo(y.leader);
  }

  private void handlePing(Ping m, Address sender) {
    CommandToProposals.clear();
    if(sender.compareTo(myAddress) > 0)
    {
      maxBallot = new Ballot(m.ballot.ballot_num, m.ballot().leader);
      newLeader = sender;
    }
    mostRecentlyPinged.add(sender);
    send(new AckMessage(myAddress, slotNumToExecute), LeaderAddress);
  }


  private void onPingCheckTimer(PingCheckTimer t) {
    mostRecentlyPinged.add(myAddress); // Include self
    boolean flag = false;
    if(!Objects.equals(LeaderAddress, newLeader) || !mostRecentlyPinged.contains(LeaderAddress)){
      flag = true;
      if(Objects.equals(LeaderAddress, newLeader))
      {
        LeaderAddress = myAddress;
        newLeader = myAddress;
      }
      else LeaderAddress = newLeader;
      set(new PingTimer(), PING_MILLIS);
    }
    ActiveServers.clear();
    ActiveServers.addAll(mostRecentlyPinged);

    mostRecentlyPinged.clear();
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    if(flag == true)
    {
      curBallotLeader = new Ballot(curBallotLeader.ballot_num, LeaderAddress);
      callScout(myAddress, Servers);
    }
  }

  private void onPingTimer(PingTimer t) {
    if(!Objects.equals(this.myAddress,LeaderAddress))return;
    Broadcast(new Ping(slotNumToExecute, curBallotLeader), Servers);
    set(new PingTimer(), PING_MILLIS);
  }

  private void handleAckMessage(AckMessage msg, Address sender)
  {
    mostRecentlyPinged.add(sender);
    if(msg.slot < slotNumToExecute)
    {
      HashMap<Integer, PaxosRequest> missingDecisions = new HashMap<>();
      for(int i = msg.slot; i<slotNumToExecute; i++)missingDecisions.put(i , SlotToCommandDecision.get(i));
      ArrayList<Address> a = new ArrayList<>();
      a.add(sender);
      Broadcast(new decisionTransfer(msg.slot, missingDecisions), a.toArray(new Address[a.size()]));
    }
    else if(msg.slot > slotNumToExecute)
    {
      send(new GetState(slotNumToExecute), sender);
    }
  }

  void handleGetState(GetState msg, Address sender)
  {
    HashMap<Integer, PaxosRequest> missingDecisions = new HashMap<>();
    for(int a : missingDecisions.keySet())if(a >= msg.slot)missingDecisions.put(a , SlotToCommandDecision.get(a));
    send(new decisionTransfer(slotNumToExecute, missingDecisions), sender);
  }

  private void handledecisionTransfer(decisionTransfer msg, Address sender)
  {
    if(msg.slot > slotNumToExecute)
    {
      for(int a : msg.decision_list.keySet())
      {
        SlotToCommandDecision.put(a,msg.decision_list.get(a));
        CommandToSlotDecision.put(msg.decision_list.get(a), a);
      }
      commandExecution();
    }
  }

  private boolean NotMe(Address x)
  {
    return true;
  }
}
