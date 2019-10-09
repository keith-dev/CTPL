/*********************************************************
*
*  Copyright (C) 2014 by Vitaliy Vitsentiy
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*********************************************************/

#ifndef __ctpl_stl_thread_pool_H__
#define __ctpl_stl_thread_pool_H__

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <exception>
#include <future>
#include <mutex>
#include <queue>



// thread pool to run user's functors with signature
//      ret func(size_t id, other_params)
// where id is the index of the thread that runs the functor
// ret is some return type


namespace ctpl {
	namespace detail {
		template <typename T>
		class Queue {
		public:
			bool push(T const & value) {
				std::unique_lock<std::mutex> lock(mutex_);
				q_.push(value);
				return true;
			}
			// deletes the retrieved element, do not use for non integral types
			bool pop(T & v) {
				std::unique_lock<std::mutex> lock(mutex_);
				if (q_.empty())
					return false;
				v = q_.front();
				q_.pop();
				return true;
			}
			bool empty() {
				std::unique_lock<std::mutex> lock(mutex_);
				return q_.empty();
			}
		private:
			std::queue<T> q_;
			std::mutex mutex_;
		};
	}

	class thread_pool {
	public:
		thread_pool() { init(); }
		thread_pool(size_t nThreads) { init(); resize(nThreads); }

		// the destructor waits for all the functions in the queue to be finished
		~thread_pool() { stop(true); }

		// get the number of running threads in the pool
		size_t size() { return threads_.size(); }

		// number of idle threads
		size_t n_idle() { return nWaiting_; }
		std::thread & get_thread(size_t i) { return *threads_[i]; }

		// change the number of threads in the pool
		// should be called from one thread, otherwise be careful to not interleave, also with stop()
		// nThreads must be >= 0
		void resize(size_t nThreads) {
			if (!isStop_ && !isDone_) {
				size_t oldNThreads = threads_.size();
				if (oldNThreads <= nThreads) {  // if the number of threads is increased
					threads_.resize(nThreads);
					flags_.resize(nThreads);

					for (size_t i = oldNThreads; i < nThreads; ++i) {
						flags_[i] = std::make_shared<std::atomic<bool>>(false);
						set_thread(i);
					}
				}
				else {  // the number of threads is decreased
					for (size_t i = oldNThreads - 1; i >= nThreads; --i) {
						*flags_[i] = true;  // this thread will finish
						threads_[i]->detach();
					}
					{
						// stop the detached threads that were waiting
						std::unique_lock<std::mutex> lock(mutex_);
						cv_.notify_all();
					}
					threads_.resize(nThreads);  // safe to delete because the threads are detached
					flags_.resize(nThreads);  // safe to delete because the threads have copies of shared_ptr of the flags, not originals
				}
			}
		}

		// empty the queue
		void clear_queue() {
			std::function<void(size_t id)> * _f;
			while (q_.pop(_f))
				delete _f; // empty the queue
		}

		// pops a functional wrapper to the original function
		std::function<void(int)> pop() {
			std::function<void(size_t id)> * _f = nullptr;
			q_.pop(_f);
			std::unique_ptr<std::function<void(size_t id)>> func(_f); // at return, delete the function even if an exception occurred
			std::function<void(int)> f;
			if (_f)
				f = *_f;
			return f;
		}

		// wait for all computing threads to finish and stop all threads
		// may be called asynchronously to not pause the calling thread while waiting
		// if isWait == true, all the functions in the queue are run, otherwise the queue is cleared without running the functions
		void stop(bool isWait = false) {
			if (!isWait) {
				if (isStop_)
					return;
				isStop_ = true;
				for (size_t i = 0, n = size(); i < n; ++i) {
					*flags_[i] = true;  // command the threads to stop
				}
				clear_queue();  // empty the queue
			}
			else {
				if (isDone_ || isStop_)
					return;
				isDone_ = true;  // give the waiting threads a command to finish
			}
			{
				std::unique_lock<std::mutex> lock(mutex_);
				cv_.notify_all();  // stop all waiting threads
			}
			for (size_t i = 0; i < threads_.size(); ++i) {  // wait for the computing threads to finish
					if (threads_[i]->joinable())
						threads_[i]->join();
			}
			// if there were no threads in the pool but some functors in the queue, the functors are not deleted by the threads
			// therefore delete them here
			clear_queue();
			threads_.clear();
			flags_.clear();
		}

		template<typename F, typename... Rest>
		auto push(F && f, Rest&&... rest) ->std::future<decltype(f(0, rest...))> {
			auto pck = std::make_shared<std::packaged_task<decltype(f(0, rest...))(int)>>(
				std::bind(std::forward<F>(f), std::placeholders::_1, std::forward<Rest>(rest)...)
				);
			auto _f = new std::function<void(size_t id)>([pck](size_t id) {
				(*pck)(id);
			});
			q_.push(_f);
			std::unique_lock<std::mutex> lock(mutex_);
			cv_.notify_one();
			return pck->get_future();
		}

		// run the user's function that excepts argument size_t - id of the running thread. returned value is templatized
		// operator returns std::future, where the user can get the result and rethrow the catched exceptins
		template<typename F>
		auto push(F && f) ->std::future<decltype(f(0))> {
			auto pck = std::make_shared<std::packaged_task<decltype(f(0))(int)>>(std::forward<F>(f));
			auto _f = new std::function<void(size_t id)>([pck](size_t id) {
				(*pck)(id);
			});
			q_.push(_f);
			std::unique_lock<std::mutex> lock(mutex_);
			cv_.notify_one();
			return pck->get_future();
		}

	private:
		// deleted
		thread_pool(const thread_pool &);// = delete;
		thread_pool(thread_pool &&);// = delete;
		thread_pool & operator=(const thread_pool &);// = delete;
		thread_pool & operator=(thread_pool &&);// = delete;

		void set_thread(size_t i) {
			std::shared_ptr<std::atomic<bool>> flag(flags_[i]); // a copy of the shared ptr to the flag
			auto f = [this, i, flag/* a copy of the shared ptr to the flag */]() {
				std::atomic<bool> & _flag = *flag;
				std::function<void(size_t id)> * _f;
				bool isPop = q_.pop(_f);
				while (true) {
					while (isPop) {  // if there is anything in the queue
						std::unique_ptr<std::function<void(size_t id)>> func(_f); // at return, delete the function even if an exception occurred
						(*_f)(i);
						if (_flag)
							return;  // the thread is wanted to stop, return even if the queue is not empty yet
						else
							isPop = q_.pop(_f);
					}
					// the queue is empty here, wait for the next command
					std::unique_lock<std::mutex> lock(mutex_);
					++nWaiting_;
					cv_.wait(lock, [this, &_f, &isPop, &_flag](){ isPop = q_.pop(_f); return isPop || isDone_ || _flag; });
					--nWaiting_;
					if (!isPop)
						return;  // if the queue is empty and isDone_ == true or *flag then return
				}
			};
			threads_[i].reset(new std::thread(f)); // compiler may not support std::make_unique()
		}

		void init() { nWaiting_ = 0; isStop_ = isDone_ = false; }

		std::vector<std::unique_ptr<std::thread>> threads_;
		std::vector<std::shared_ptr<std::atomic<bool>>> flags_;
		detail::Queue<std::function<void(size_t id)> *> q_;
		std::atomic<bool> isDone_;
		std::atomic<bool> isStop_;
		std::atomic<size_t> nWaiting_;  // how many threads are waiting

		std::mutex mutex_;
		std::condition_variable cv_;
	};
}

#endif // __ctpl_stl_thread_pool_H__
