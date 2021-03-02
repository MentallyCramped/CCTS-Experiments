#include <iostream>
#include <random>
#include <vector>
#include <unordered_map>
#include <thread>
#include <fstream>
#include <mutex>
#include <chrono>
#include <shared_mutex>
#include <set>
using namespace std;

typedef unsigned long long ll;
typedef vector<ll> vll;
typedef vector< vector<ll>> vvll;
typedef set<ll> sll;
vector<thread> threads;
ll max_threads = thread::hardware_concurrency(), tcount = 0, num_vars = 0, commit_time = 0;
mutex tcount_l, printl, valwritel, statl;
shared_mutex live_set_l;
vector<shared_mutex> var_lock, gwritel;      //lock for global memory and gwrite
vll live_set, var_value, committed;
vvll gwrite, gread;
auto begin_time = chrono::steady_clock::now();
//unordered_map<ll, ll> timestamp;
vll timestamp(100000,0);

string gettime(){
    time_t t = time(NULL);
    return ctime(&t);
}

ll read(ll var, ll tid, vll &ts){
    var_lock[var].lock_shared();
//    gread[var].push_back(tid);
    ll val = var_value[var];
    //Print Locks are unnecessary if the algorithm works
//    printl.lock();
//    cout << "Trans " << tid <<" reads " << var << " with value " << val << " at time " << gettime() << endl;
//    printl.unlock();
    var_lock[var].unlock_shared();
    ts[var] = (chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now() - begin_time).count());
    return val;
}

bool intersect(vll a, vll b, ll rvar, const vll &ts){
    if(a.size() < b.size()) swap(a,b);
    unordered_map<ll, ll> ht;
    for(int i = 0; i < a.size(); i++){
        ht[a[i]] = 1;
    }
    for(int i = 0; i < b.size(); i++){
        if(ht.find(b[i]) != ht.end() && timestamp[b[i]] > ts[rvar]) {
            return true;
        }
    }
    ht.clear();
    return false;
}

/*
 * each tid should have their commit time
 */

bool val_write(const sll &RS, const sll &WS, ll tid, vll live_set_begin, const vll &ts){
    valwritel.lock();
//    printl.lock();
//    cout << "Trans " << tid <<" enters val write at time " << gettime() << endl;
//    printl.unlock();
    bool abort = false;
    for(auto it = RS.begin(); it != RS.end(); it++){
        ll rvar = *it;
        gwritel[rvar].lock_shared();
        if(intersect(live_set_begin, gwrite[rvar], rvar, ts)) {
            abort = true;
            gwritel[rvar].unlock_shared();
            break;
        }
        gwritel[rvar].unlock_shared();
    }
    if(!abort){
        for(auto it = WS.begin(); it != WS.end(); it++){
            ll wvar = *it;
            var_lock[wvar].lock();
            gwritel[wvar].lock();
            gwrite[wvar].push_back(tid);    //update gwrite
            var_value[wvar] = tid;  //write random value
//            printl.lock();
//            cout << "Trans " << tid <<" writes " << wvar << " with value " << tid << " at time " << gettime() << endl;
//            printl.unlock();
            gwritel[wvar].unlock();
            var_lock[wvar].unlock();
        }
        timestamp[tid] = (chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now() - begin_time).count());
    }
    valwritel.unlock();
    return abort;
}

void func(ll tid, string trans){
    vll ts(num_vars + 1);
//    unordered_map<ll, ll> ts;
//    printl.lock();
//    cout << "Trans " << tid << " has started at time " << gettime() <<endl;
//    printl.unlock();
    auto start = chrono::steady_clock::now();
    while(true) {
        live_set_l.lock_shared();
        vll live_set_begin(live_set);
        live_set_l.unlock_shared();
        vll localvar(num_vars, 0);
        sll RS, WS;
        int i = 0;
        while (i < trans.size()) {
            char op = trans[i];
            i++;
            //There's possibly a bug here.
            ll var = 0;
            while (trans[i] != ',' && trans[i] != '.') {
                var = var * 10 + (trans[i] - '0');
                i++;
            }
            i++;
            if (op == 'r') {
                if (RS.contains(var)) break;
                RS.insert(var);
                localvar[var] = read(var, tid, ts);
            } else {
                WS.insert(var);
                localvar[var] = 0;
            }
        }
        bool abort = val_write(RS, WS, tid, live_set_begin, ts);
        if(!abort){
//            printl.lock();
//            cout << "Trans " << tid << " has committed at time " << gettime();
//            printl.unlock();
            break;
        }
        else{
//            printl.lock();
//            cout << "Trans " << tid << " has aborted at time " << gettime() << ". Restarting thread!" << endl;
//            printl.unlock();
        }
    }
    auto end = chrono::steady_clock::now();
    statl.lock();
    commit_time += chrono::duration_cast<chrono::nanoseconds>(end - start).count();
    statl.unlock();
    live_set_l.lock();
    live_set.erase(lower_bound(live_set.begin(), live_set.end(), tid));
    live_set_l.unlock();
    tcount_l.lock();
    tcount--;
    tcount_l.unlock();
    //handle gc signals
}

int main(){
    ll prev_time = 0;
    for(int test = 15; test < 25; test++) {
        //clear global variables
        gwrite.clear();
        var_lock.clear();
        var_value.clear();
        gwritel.clear();
//        live_set.clear();
//        timestamp.clear();

        //Init code
        fstream in;
        in.open("C:\\cygwin64\\CCTS_reimpl\\benchmark\\random_test_" + to_string(test) + ".txt", ios::in);
        ll tid = 0;
        string temp;
        in >> temp;
        num_vars = stoll(temp);
        for (int i = 0; i < num_vars; i++) {
            gwrite.push_back({});   //{} is an empty vector
//            gread.push_back({});
            var_value.push_back(0);
        }
        vector<shared_mutex> empty(num_vars), empty2(num_vars);
        var_lock.swap(empty);
        gwritel.swap(empty2);

        vector<thread> threads;

        //Reading all the transactions
        while (!in.eof()) {
            while (tcount == max_threads * 2);    //we haven't used locks here, but it works in reality
            tid++;
            tcount_l.lock();
            tcount++;
            tcount_l.unlock();
            string trans;
            in >> trans;
            live_set_l.lock();
            live_set.push_back(tid);
            live_set_l.unlock();
            threads.push_back(move(thread(func, tid, trans)));
        }
        in.close();
        for(ll i = 0; i < threads.size(); i++) threads[i].join();
        cout << "All threads have terminated! Average commit time in millisecs for file " << test << " is " << (commit_time - prev_time)/(2000*1000000) << endl;
        prev_time = commit_time;
    }
//    cout << "All files are done! Average commit time in millisecs " << commit_time/(2000*1000*10) << endl;
}