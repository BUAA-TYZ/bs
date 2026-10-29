#!/usr/bin/env python3


import sys
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class Contact:
    frm: int
    to: int
    to_eid: int = None
    start: int | float = 0.0
    end: int | float = sys.maxsize
    rate: int | float = 1
    confidence: float = 1.0
    owlt: float = 0.0
    # route search working area
    arrival_time: int | float = sys.maxsize
    visited: bool = False
    visited_nodes: List = field(default_factory=list)
    predecessor: int = 0
    # route management working area
    suppressed: bool = False
    suppressed_next_hop: List = field(default_factory=list)
    # forwarding working area
    first_byte_tx_time: int | float = None
    last_byte_tx_time: int | float = None
    last_byte_arr_time: int | float = None
    effective_volume_limit: int | float = None

    def __post_init__(self):
        # TODO is this really necessary? We're using it so that Tasks know the contacts
        #  used in the acquisition and delivery paths, rather than storing pointers to
        #  actual contacts. This is because a Task is really just an item in a table
        #  that gets passed around, so it doesn't really make sense to point to a Contact
        self.__uid = f"{self.frm}_{self.to}_{self.start}"
        self.volume = self.rate * (self.end - self.start)
        
        # Priority-specific volume. Index represents priority level, so mav[0] = "bulk"
        self.mav = [self.volume, self.volume, self.volume]

        if not self.to_eid:
            self.to_eid = self.to

    @property
    def uid(self):
        return self.__uid

    def clear_dijkstra_area(self):
        self.arrival_time = sys.maxsize
        self.visited = False
        self.predecessor = 0
        self.visited_nodes = []

    def clear_management_area(self):
        self.suppressed = False
        self.suppressed_next_hop = []

    def __lt__(self, other):
        # 1st priority is the start time. An earlier start time is deemed higher priority
        if self.start < other.start:
            return True

        # 2nd priority is duration, a shorter contact is deemed higher priority
        elif self.start == other.start:
            if self.end - self.start < other.end - other.start:
                return True

            # 3rd priority is confidence level, where higher confidence = higher priority
            elif self.end - self.start == other.end - other.start:
                if self.confidence > other.confidence:
                    return True

        return False

    def __repr__(self):

        # replace with inf
        if self.end == sys.maxsize:
            end = "inf"
        else:
            end = self.end

        # mav in %
        if self.volume <= 0:
            volume = 0
        else:
            volume = 100 * min(self.mav) / self.volume

        return "%s->%s (%s-%s, owlt:%s) [vol:%d]" % (self.frm, self.to, self.start, end,
                                                self.owlt, volume)


class Route:
    def __init__(self, contact):
        """
        A Route is an ordered sequence of contact events.

        The capacity of each contact, combined with the availability (and other factors
        such as the available storage capacity on board the receiving node) will
        dictate the feasibility of the journey for a particular bundle.

        :param contact: Contact object
        """
        self._hops = []
        self.volume = None
        self.append(contact)

    @property
    def hops(self):
        """
        Ordered sequence of Contacts that make up the route
        :return:
        """
        return self._hops

    @property
    def confidence(self):
        avail = 1
        for x in self._hops:
            avail *= x.confidence
        return avail

    # TODO move this code out of the volume getter so that we're not having to execute
    #  EVERY time we want the volume. The volume only needs recalculating when we
    #  append a contact, so we can just refresh it then (and if we pop the root contact)
    def refresh_metrics(self):
        prev_last_byte_arr_time = 0
        min_effective_volume_limit = sys.maxsize
        for c in self.hops:
            if c == self.hops[0]:
                c.first_byte_tx_time = c.start
            else:
                c.first_byte_tx_time = max(c.start, prev_last_byte_arr_time)
            bundle_tx_time = 0  # immediate transmission
            c.last_byte_tx_time = c.first_byte_tx_time + bundle_tx_time
            c.last_byte_arr_time = c.last_byte_tx_time + c.owlt
            prev_last_byte_arr_time = c.last_byte_arr_time

            effective_start_time = c.first_byte_tx_time
            min_succ_stop_time = sys.maxsize
            index = self.hops.index(c)
            for successor in self.hops[index:]:
                if successor.end < min_succ_stop_time:
                    min_succ_stop_time = successor.end
            effective_stop_time = min(c.end, min_succ_stop_time)
            effective_duration = effective_stop_time - effective_start_time
            # TODO c.volume is not traditionally updated with the assignment of
            #  bundles, so this therefore isn't normally dynamic in that sense.
            c.effective_volume_limit = min(effective_duration * c.rate, c.volume)
            if c.effective_volume_limit < min_effective_volume_limit:
                min_effective_volume_limit = c.effective_volume_limit
        self.volume = min_effective_volume_limit

    @property
    def best_delivery_time(self):
        # Best-case delivery time (i.e. the earliest time a byte of data could arrive
        # at the destination)
        bdt = 0
        for c in self.hops:
            bdt = max(bdt + c.owlt, c.start + c.owlt)
        return bdt

    @property
    def to_time(self):
        to_time = sys.maxsize
        for c in self.hops:
            to_time = min(to_time, c.end)
        return to_time

    @property
    def next_node(self):
        if self.hops:
            return self.hops[0].to
        return None

    @property
    def to_node(self):
        if self.hops:
            return self.hops[-1].to
        return None

    @property
    def from_time(self):
        if self.hops:
            return self.hops[0].start
        return 0

    def append(self, contact):
        """
        Add a hop to the journey
        :return:
        """
        self._hops.append(contact)
        self.refresh_metrics()

    # OPERATOR OVERLOAD FOR SELECTION #
    # Less than = this route is better than the other (less costly)
    def __lt__(self, other_route):
        # 1st priority : arrival time
        if self.best_delivery_time < other_route.best_delivery_time:
            return True

        # FIXME This should prioritise routes that have earlier intermediate contacts.
        #  Currently, a route containing 3 contacts, with the middle one coming later,
        #  could be deemed better than one with the middle contact earlier.
        # 2nd: volume
        elif self.best_delivery_time == other_route.best_delivery_time:
            if self.volume > other_route.volume:
                return True

            # 3rd: confidence
            elif self.volume == other_route.volume:
                if self.confidence >= other_route.confidence:
                    return True

        return False

    def __repr__(self):
        return "to:%s | via:%s | bdt:%s | hops:%s | volume:%s | conf:%s" % \
               (
                   self.hops[-1].to,
                   self.hops[0].to,
                   self.best_delivery_time,
                   len(self.hops),
                   self.volume,
                   self.confidence
               )

    # TODO add an __add__ dunder method as an alt to "append". This requires
    #  us to consider the first route as the "parent" and should be passed in as an arg.
    # def __add__(self, contact)
    #     return Route(contact, self)


def cgr_yens(
        src: int,
        dest: int,
        contact_plan: List[Contact],
        t_now: float = 0.0,
        num_routes: int = 10,
        routes: List[Route] = None
) -> List[Route]:
    """Find the k shortest paths between nodes s & d in the graph, g.
    """
    if routes is None:
        routes = []

    potential_routes = []

    # Root contact is the connection to self that acts as the source vertex in the
    # Contact Graph
    root = Contact(src, src, src, t_now, sys.maxsize, sys.maxsize)
    root.arrival_time = t_now

    if not routes:
        # reset contacts
        for contact in contact_plan:
            contact.clear_dijkstra_area()
            contact.clear_management_area()

        # Find the lowest cost path using Dijkstra
        route = cgr_dijkstra(root, dest, contact_plan)

        if route is None:
            return routes

        routes.append(route)

    [r.hops.insert(0, root) for r in routes]

    for k in range(num_routes - 1):
        # For each contact in the most recently identified (k-1'th) route (apart
        # from the last contact
        for spur_contact in routes[-1].hops[:-1]:

            # create root_path that follows the most recently found route from the
            # root_contact up to and including the spur_contact. It is fron this
            # point that we'll branch off and do a new search.
            spur_contact_index = routes[-1].hops.index(spur_contact)

            root_path = Route(routes[-1].hops[0])
            for hop in routes[-1].hops[1:spur_contact_index + 1]:
                root_path.append(hop)

            # reset contacts
            for contact in contact_plan:
                contact.clear_dijkstra_area()
                contact.clear_management_area()

            # suppress all contacts in root_path except spur_contact
            for contact in root_path.hops[:-1]:
                contact.suppressed = True

            # suppress outgoing edges from spur_contact covered by known routes
            for route in routes:
                if root_path.hops == route.hops[0:(len(root_path.hops))]:
                    if route.hops[
                        len(root_path.hops)
                    ] not in spur_contact.suppressed_next_hop:
                        spur_contact.suppressed_next_hop.append(
                            route.hops[len(root_path.hops)]
                        )

            # prepare spur_contact as root contact
            spur_contact.clear_dijkstra_area()
            spur_contact.arrival_time = root_path.best_delivery_time
            # spur_contact.cost = root_path.cost
            for hop in root_path.hops:  # add visited nodes to spur_contact
                spur_contact.visited_nodes.append(hop.to)

            # try to find a spur_path with dijkstra
            spur_path = cgr_dijkstra(spur_contact, dest, contact_plan)

            # if found store new route in potential_routes
            if spur_path:
                total_path = Route(root_path.hops[0])
                for hop in root_path.hops[1:]:  # append root_path
                    total_path.append(hop)
                for hop in spur_path.hops:  # append spur_path
                    total_path.append(hop)
                # [NEW] Without this, there's a risk of getting repeated routes found
                if total_path.hops not in [p.hops for p in potential_routes]:
                    potential_routes.append(total_path)

        # if no more potential routes end search
        if not potential_routes:
            break

        # sort potential routes by arrival_time
        potential_routes.sort()

        # add best route to routes
        routes.append(potential_routes.pop(0))

    # remove root_contact from hops
    for route in routes:
        route.hops.pop(0)
        route.refresh_metrics()

    return routes


def cgr_dijkstra(
        root_contact, destination, contact_plan, deadline=sys.maxsize, size=0
) -> Route | None:
    """
    Finds the lowest cost Route from the current node to a destination node
    :return:
    """
    # If there are no contacts from our root (i.e. there's nowhere for us to go), exit
    if root_contact.to not in [c.to for c in contact_plan]:
        return

    [c.clear_dijkstra_area() for c in contact_plan if c is not root_contact]

    contact_plan_hash = {}
    for contact in contact_plan:
        if contact.frm not in contact_plan_hash:
            contact_plan_hash[contact.frm] = []
        if contact.to not in contact_plan_hash:
            contact_plan_hash[contact.to] = []
        contact_plan_hash[contact.frm].append(contact)

    # Pre-set the variables used to track the "optimal" route and set the arrival
    # time along the "best" route (the "best delivery time", bdt) to be large
    route = None  # optimal route so far
    final_contact = None  # The "final" contact along the current route
    earliest_fin_arr_t = sys.maxsize  # "best delivery time"

    current = root_contact
    # TODO Identify a situation when this would ever NOT be the case, seems redundant
    if root_contact.to not in root_contact.visited_nodes:
        root_contact.visited_nodes.append(root_contact.to)

    while True:
        try:
            contact_plan_hash[current.to]
        except:
            print(' ')
        for contact in contact_plan_hash[current.to]:
            if contact in current.suppressed_next_hop:
                continue
            if contact.suppressed:
                continue
            if contact.visited:
                continue
            if contact.to in current.visited_nodes:
                continue
            # [NEW] Check that this contact is even worth looking at for our task
            if contact.start >= deadline:
                continue

            # TODO This is new, triple check I'm right here
            if not contact.rate:
                print('')
            transfer_time = size / contact.rate
            if contact.end <= current.arrival_time + transfer_time:
                continue

            # While there may be sufficient time available to send the bundle,
            # there may not actually be a sufficient amount of volume remaining.
            # TODO this needs considered in terms of priority, since there may be
            #  volume for high priority bundles, but not low-priority ones.
            if contact.volume < size:
                continue

            # TODO remove this as should never be the case I don't think
            if current.frm == contact.to and current.to == contact.frm:
                continue

            # Calculate arrival time (cost) - I.e. the time at which the first byte of
            # data can arrive at the receiving node
            # If the next contact begins before we could have arrived at the
            # current contact, set the arrival time to be the arrival at the
            # current plus the time to traverse the contact
            # Calculate arrival time (cost)
            if contact.start < current.arrival_time:
                arrvl_time = current.arrival_time + contact.owlt
            else:
                arrvl_time = contact.start + contact.owlt

            # Update cost if better and update other parameters in
            # next contact so that we assume we're coming from the current
            # NOTE: CGR has this as less than or equal to, but why update if "equal
            # to"? In fact, in "Routing in the Space Internet: A contact graph routing
            # tutorial", it's "<" (Algorithm 2, line 17)
            if arrvl_time < contact.arrival_time:
                contact.arrival_time = arrvl_time
                contact.predecessor = current
                contact.visited_nodes = current.visited_nodes[:]
                contact.visited_nodes.append(contact.to)

                # Mark if destination reached
                # if contact.to == destination and contact.arrival_time < earliest_fin_arr_t:
                if contact.to_eid == destination and contact.arrival_time < earliest_fin_arr_t:
                    earliest_fin_arr_t = contact.arrival_time
                    final_contact = contact

        # This completes our assessment of the current contact
        current.visited = True

        # Determine best next contact among all in contact plan
        earliest_arr_t = sys.maxsize
        next_contact = None

        for contact in contact_plan:

            # Ignore visited or suppressed
            if contact.visited or contact.suppressed:
                continue

            # If we know there is another, better contact, continue
            if contact.arrival_time > earliest_fin_arr_t:
                continue

            if contact.arrival_time < earliest_arr_t:
                earliest_arr_t = contact.arrival_time
                next_contact = contact

        if not next_contact:
            break
        current = next_contact

    # Done contact graph exploration, check and store new route
    if final_contact is not None:
        hops = []
        contact = final_contact
        while contact != root_contact:
            hops.insert(0, contact)
            contact = contact.predecessor

        route = Route(hops[0])
        for hop in hops[1:]:
            route.append(hop)

    return route


def contact_review(contact_plan, current, dest, final_contact, bdt,
                   deadline=sys.maxsize, size=0):
    """
    Review each contact that is adjacent to the current one (i.e. the sending node
    of the next contact = the receiving node of the current one) and update the
    best case arrival time. If any of the contacts have the destination as the
    receiving node, mark as "final" and log the time at which we could arrive
    there. Return the "final node" and the time of arrival at that node (delivery).
    :param current: Current contact from which we are searching
    :param dest: Destination node for the route being constructed
    :param final_contact: Adjacent contact with earliest arrival time and
        destination as the receiving node
    :param bdt: Best case delivery time at the destination via the "final" contact
    :return final_contact:
    :return bdt:
    """
    # FIXME Shouldn't need to initialise the contact_plan_hash like this if we have a
    #  full contact plan. This is only required in the odd case where one of the nodes
    #  isn't actually involved in any contacts. In that case, they don't get added to
    #  the CPH and therefore the root contact (i.e. "current" may cause a failure)
    contact_plan_hash = {current.frm: []}
    for contact in contact_plan:
        if contact.frm not in contact_plan_hash:
            contact_plan_hash[contact.frm] = []
        if contact.to not in contact_plan_hash:
            contact_plan_hash[contact.to] = []
        contact_plan_hash[contact.frm].append(contact)

    for contact in contact_plan_hash[current.to]:
        if contact in current.suppressed_next_hop:
            continue
        if contact.suppressed or contact.visited:
            continue
        if contact.to in current.visited_nodes:
            continue
        if contact.end <= current.arrival_time:
            continue
        # FIXME This only considers the total volume on the contact, but if we arrive
        #  part way through (e.g. due to overlap), we might over-estimate how much
        #  volume there really is
        if contact.volume < size:
            continue
        # TODO remove this as should never be the case I don't think
        if current.frm == contact.to and current.to == contact.frm:
            continue
        # TODO this is the absolute best case arrival time, really should be
        #  considering the first-byte-transmission time here, rather than "start"
        if contact.start+contact.owlt > deadline:
            continue

        # Calculate arrival time (cost)
        # If the next contact begins before we could have arrived at the
        # current contact, set the arrival time to be the arrival at the
        # current plus the time to traverse the contact
        arrvl_time = max(
            current.arrival_time + contact.owlt,
            contact.start + contact.owlt
        )

        # Update cost if better or equal and update other parameters in
        # next contact so that we assume we're coming from the current
        # if cost < contact.cost:
        if arrvl_time < contact.arrival_time:
            # contact.cost = cost
            contact.arrival_time = arrvl_time
            contact.predecessor = current
            contact.visited_nodes = current.visited_nodes[:]
            contact.visited_nodes.append(contact.to)

            # Mark if destination reached
            if contact.to == dest and contact.arrival_time < bdt:
                bdt = contact.arrival_time
                final_contact = contact

    # This completes our assessment of the current contact
    current.visited = True
    return final_contact, bdt


def contact_selection(contact_plan, bdt, deadline):
    # Determine best next contact among all in contact plan
    earliest_arr_t = sys.maxsize
    next_contact = None

    for contact in contact_plan:

        # Ignore visited or suppressed
        if contact.visited or contact.suppressed:
            continue

        # TODO Triple check this is ok. In the pycgr implementation they just
        #  "continue" here, and go on to look at the rest of the CP, but I don't
        #  think this is necessary if the CP is ordered based on contact arrival
        #  time. I suppose if we don't want to order the CP, then we'd need to look
        #  through it all every time.
        # If we know there is another, better contact, break from the search as
        # nothing else in the CP is going to be better
        if bdt < contact.arrival_time < sys.maxsize-1:
            break

        if contact.arrival_time < earliest_arr_t:
            earliest_arr_t = contact.arrival_time
            next_contact = contact

    return next_contact


def candidate_routes(curr_time, curr_node, contact_plan, bundle, routes,
                     excluded_nodes, obq=None, debug=False):

    return_to_sender = True
    candidate_routes = []

    for route in routes:

        # 3.2.5.2 a) preparation: backward propagation
        if not return_to_sender:
            if route.next_node is bundle.previous_node:
                excluded_nodes.append(route.next_node)
                if debug:
                    print("preparation: next node is sender", route.next_node)
                continue

        # 3.2.6.9 a)
        if route.best_delivery_time > bundle.deadline:
            if debug:
                print("not candidate: best delivery time (bdt) is later than deadline")
            continue

        # 3.2.6.9 b)
        if route.next_node in excluded_nodes:
            if debug:
                print("not candidate: next node in excluded nodes list")
            continue

        # 3.2.6.9 c)
        for contact in route.hops:
            if contact.to is curr_node:
                if debug:
                    print("not candidate: contact in route tx to current node")
                continue

            # *** ADDED ***
            # Check whether each contact has enough volume to accommodate the bundle,
            # regardless of arrival times etc. This assumes we can make use of whatever
            # volume exists on the contact, despite the fact that, in the case of
            # over-lapping contacts, this won't always be true. However, this offers a
            # fast check to make sure we're not over-subscribing the contact,
            # in a best-case scenario.
            if contact.mav[bundle.priority] < bundle.size:
                # FIXME This doesn't actually do anything as inside the for loop. Same
                #  for the one above...
                continue

        # 3.2.6.9 d) calculate eto and if it is later than 1st contact end time, ignore
        # This basically just looks at the current bundle allocation to the route's
        # "next node" and, if there's already enough bundles in the queue to fill up
        # the first hop's capacity (assuming other, earlier contacts with this first
        # hop's recipient), we can ignore this node.
        adjusted_start_time = max(curr_time, route.hops[0].start)  # line 4

        # Applicable Backlog Volume (for priority p), i.e. this is the volume of data
        # currently in the transmission queue (OBQ) for the route's "next node",
        # who's priority (p) is greater than, or equal to, that of this bundle
        # TODO account for priority in the backlog calculation. This is not accounted
        #  for in Algorithm 5 either, it is just called out without definition. As
        #  such, this is something that needs to be continuously updated based on the
        #  assignment of bundles.
        if obq:
            applicable_backlog_p = sum(
                b.size for b in obq[route.next_node] if b.priority >= bundle.priority)
        else:
            applicable_backlog_p = 0

        applicable_backlog_relief = 0  # line 5 (v_prior)
        for contact in contact_plan:
            if contact.frm == route.hops[0].frm and contact.to == route.hops[0].to:
                if contact.end > curr_time and contact.start < route.hops[0].start:
                    # How much of the contact is remaining (from now)?
                    applicable_duration = contact.end - max(curr_time, contact.start)
                    # How much data can we fit over this contact (assuming its clear)?
                    applicable_prior_contact_volume = applicable_duration * contact.rate
                    # What is the total backlog "relief"
                    applicable_backlog_relief += applicable_prior_contact_volume  # line 7
        residual_backlog = max(0, applicable_backlog_p - applicable_backlog_relief)
        backlog_lien = residual_backlog / route.hops[0].rate  # line 8
        early_tx_opportunity = adjusted_start_time + backlog_lien  # line 9
        if early_tx_opportunity > route.hops[0].end:  # line 10
            if debug:
                print(
                    "not candidate: earlier transmission opportunity is later than end of 1st contact")
            continue

        # *** ADDED ***
        # Based on what is already allocated to each hop, i.e. the remaining volume,
        # the amount of nominal volume remaining, and the amount of capacity required
        # to transfer this bundle, check that this route is feasible.

        # 3.2.6.9 e) use eto to compute projected arrival time
        # Basically, calculates, for each hop in the route, the contact's timings for
        # when data can leave one node and arrive at the next, based on the arrival at
        # the previous node and OWLT. It does not consider already-assigned bundles on
        # each contact (apart from the first one, from the ETO calculation above),
        # but simply looks at arrival and departure times.
        # TODO: This assumes contacts beyond the first hop are fully available,
        #   i.e. no consideration of remaining volume on these contacts.
        prev_last_byte_arr_time = 0  #
        for contact in route.hops:
            if contact == route.hops[0]:
                # This is different in the route volume property calculation, where it
                # just uses the contact start time as the ETO. However, if we used the
                # actual time as the "prev_last_byte_arr_time" variable, this would
                # continuously update to match this. The issue is we're not providing
                # the current time as input to the Route Volume calculation...
                contact.first_byte_tx_time = early_tx_opportunity  # line 12
            else:
                # line 14
                contact.first_byte_tx_time = max(contact.start, prev_last_byte_arr_time)
            # In the route volume calculation, this assumes a 0-size bundle, so that we
            # get the FULL available volume, rather than being bundle-size specific.
            # This is fine though, because all we need to do is see if the volume is
            # equal to or larger than the bundle size.
            bundle_tx_time = bundle.size / contact.rate
            contact.last_byte_tx_time = contact.first_byte_tx_time + bundle_tx_time  # 15
            contact.last_byte_arr_time = contact.last_byte_tx_time + contact.owlt  # 16
            prev_last_byte_arr_time = contact.last_byte_arr_time
        # This is not done in the Route volume calculation, however does that matter?
        # This gives us the PAT, i.e. the time at which the whole bundle finishes
        # arriving at the destination. I don't see why we can't have something like
        # this included. Again, we're not considering any queued traffic in the system
        # in our Route volume computation, so our PAT would not be accurate.
        proj_arr_time = prev_last_byte_arr_time  # lin 17
        if proj_arr_time > bundle.deadline:  # 18
            if debug:
                print("not candidate: projected arrival time is later than deadline")
            continue

        # 3.2.6.9 f) if route depleted for bundle priority P, ignore
        # todo: sum of al bundle.evc with p or higher that were forwarded via this route
        # reserved_volume_p = 0
        min_effective_volume_limit = sys.maxsize
        for contact in route.hops:
            # if reserved_volume_p >= contact.volume:
            #     if debug:
            #         print("not candidate: route depleted for bundle priority")
            #     continue

            # This checks to make sure this contact isn't ending before we've had an
            # opportunity to send over the previous contact. E.g. if the first contact is
            # from time 0-5, but the second is from 2-4, then this route isn't feasible
            # if we only have the final time period in the original contact remaining.
            effective_start_time = contact.first_byte_tx_time
            min_succ_stop_time = sys.maxsize
            index = route.hops.index(contact)
            for successor in route.hops[index:]:
                min_succ_stop_time = min(successor.end, min_succ_stop_time)  # 21
            effective_stop_time = min(contact.end, min_succ_stop_time)  # 22
            effective_duration = effective_stop_time - effective_start_time  # 23

            # The effective volume limit is either the amount of data that can be
            # transferred between the contact's earliest start time and its latest end
            # time, or the "known" volume on the contact (not considering timings).
            # E.g. if we've almost filled a contact with bundle assignments,
            # the contact volume may be less than the best case "real" scenario,
            # so accept that as our "limit"
            contact.effective_volume_limit = min(
                effective_duration * contact.rate, contact.mav[bundle.priority])  # line 24
            if contact.effective_volume_limit < min_effective_volume_limit:
                min_effective_volume_limit = contact.effective_volume_limit  # line 25

        # Make sure this "on-the-fly" route volume calculation is sufficient to support
        # the bundle we're  trying to send
        route_volume_limit = min_effective_volume_limit
        # TODO Why not the minimum size that a bundle can be when fragmented (100bits?)
        if route_volume_limit <= 0:
            if debug:
                print("not candidate: route is depleted for the bundle priority")
            continue

        # 3.2.6.9 g) if frag is False and route rvl(P) < bundle.evc, ignore
        if not bundle.fragment:
            # TODO switch back to EVC when we incorporate that into it, but need to
            #  flow that through to volume demand
            if route_volume_limit < bundle.size:  # bundle.evc:
                if debug:
                    print(
                        "not candidate: route volume limit is less than bundle evc and no fragment allowed")
                continue

        if debug:
            print("new candidate:", route)
        candidate_routes.append(route)

    candidate_routes.sort()

    return candidate_routes
