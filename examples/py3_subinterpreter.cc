#include <thread>

#include <Python.h>

// initialize and clean up python
struct initialize {
  initialize() {
    Py_InitializeEx(1);
    PyEval_InitThreads();
  }

  ~initialize() {
    Py_Finalize();
  }
};

// allow other threads to run
class enable_threads {
 public:
  enable_threads() {
    _state = PyEval_SaveThread();
  }

  ~enable_threads() {
    PyEval_RestoreThread(_state);
  }

 private:
  PyThreadState* _state;
};

// acquire and release the GIL
struct gil_lock {
  gil_lock() {
    PyEval_AcquireLock();
  }

  ~gil_lock() {
    PyEval_ReleaseLock();
  }
};

// restore the thread state when the object goes out of scope
class restore_tstate {
 public:
  restore_tstate() {
    _ts = PyThreadState_Get();
  }

  ~restore_tstate() {
    PyThreadState_Swap(_ts);
  }

 private:
  PyThreadState* _ts;
};

// swap the current thread state with ts, restore when the object goes out of scope
class swap_tstate {
 public:
  swap_tstate(PyThreadState* ts) {
    _ts = PyThreadState_Swap(ts);
  }

  ~swap_tstate() {
    PyThreadState_Swap(_ts);
  }

 private:
  PyThreadState* _ts;
};

// create new thread state for interpreter interp, clean up on destruction
class thread_state {
 public:
  thread_state(PyInterpreterState* interp) {
    _ts = PyThreadState_New(interp);
  }

  ~thread_state() {
    if (_ts) {
      PyThreadState_Clear(_ts);
      PyThreadState_Delete(_ts);

      _ts = nullptr;
    }
  }

  operator PyThreadState*() {
    return _ts;
  }

 private:
  PyThreadState* _ts;
};

// represent a sub interpreter
class sub_interpreter {
 public:
  // perform the necessary setup and cleanup for a new thread running using a specific interpreter
  struct thread {
    gil_lock _lock;
    thread_state _state;
    swap_tstate _swap;

    thread(PyInterpreterState* interp) : _state(interp), _swap(_state) {
    }
  };

  sub_interpreter() {
    restore_tstate restore;

    _ts = Py_NewInterpreter();
  }

  ~sub_interpreter() {
    if (_ts) {
      swap_tstate sts(_ts);

      Py_EndInterpreter(_ts);
    }
  }

  PyInterpreterState* interp() {
    return _ts->interp;
  }

 private:
  PyThreadState* _ts;
};

char program[] = R"(
import sys;

for i in range(1, 10000):
  for j in range(2, i):
     m = i % j

)";

char main_program[] =
R"(
import sys;

print("ARGV: bar");

)";

// run in a new thread
void f(PyInterpreterState* interp) {
  sub_interpreter::thread scope(interp);

  PyRun_SimpleString(program);
}

int main() {
  initialize init;

  sub_interpreter s1;
  sub_interpreter s2;
  sub_interpreter s3;

  std::thread t1(f, s1.interp());
  std::thread t2(f, s2.interp());
  std::thread t3(f, s3.interp());

  std::thread t4(f, s1.interp());

  PyRun_SimpleString(main_program);

  enable_threads t;

  t1.join();
  t2.join();
  t3.join();
  t4.join();

  return 0;
}
