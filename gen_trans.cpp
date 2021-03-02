#include <iostream>
#include <random>
#include <vector>
#include <unordered_map>
#include <cstdio>
#include <fstream>
using namespace std;

typedef long long ll;
typedef vector<ll> vll;
typedef vector<string> vs;
void generate_random_transactions(ll trans, ll vars);
void generate_skewed_transactions(ll num_trans, ll num_vars);
random_device rd;  //Will be used to obtain a seed for the random number engine
mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()

int main() {
    ll num_trans, num_vars;
    scanf("%lld %lld", &num_trans, &num_vars);
    generate_random_transactions(num_trans, num_vars);
    generate_skewed_transactions(num_trans, num_vars);
}

void generate_skewed_transactions(ll num_trans, ll num_vars) {
    ll write_percentage;
//    scanf("%lld", &write_percentage);
}

void generate_random_transactions(ll num_trans, ll num_vars) {
    uniform_int_distribution<> vars(0, num_vars - 1), rw(0, 1), len(100, 1000);
    for(int i = 0; i < 50; i++) {
        vs trans(num_trans);
        fstream out;
        out.open("C:\\cygwin64\\CCTS_reimpl\\benchmark\\random_test_" + to_string(i) + ".txt", ios::out);
        out << to_string(num_vars) << endl;
        for (int i = 0; i < num_trans; i++) {
            ll length = len(gen);
            if (!length % 2) length++;
            for (int j = 0; j < length; j += 2) {
                if (rw(gen) <= 0.5) trans[i].push_back('r');
                else trans[i].push_back('w');
                trans[i].append(to_string(vars(gen)));
                if (j < length - 2)trans[i].push_back(',');
                else trans[i].push_back('.');
            }
//        cout << trans[i] << endl;
            out << trans[i] << endl;
        }
        out.clear();
        out.close();
    }
}