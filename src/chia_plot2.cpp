/*
 * chia_plot.cpp
 *
 *  Created on: Jun 5, 2021
 *      Author: mad
 */

#include <chia/phase1.hpp>
#include <chia/phase2.hpp>
#include <chia/phase3.hpp>
#include <chia/phase4.hpp>
#include <chia/util.hpp>
#include <chia/copy.h>

#include <bls.hpp>
#include <sodium.h>
#include <cxxopts.hpp>
#include <sys/statvfs.h>
#include <string>
#include <csignal>

#include <stdio.h>
#include <dirent.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <ctime>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#ifdef __linux__ 
	#include <unistd.h>
	#define GETPID getpid
#elif _WIN32
	#include <processthreadsapi.h>
	#define GETPID GetCurrentProcessId
#else
	#define GETPID() int(-1)
#endif

bool gracefully_exit = false;
int64_t interrupt_timestamp = 0;

static void interrupt_handler(int sig)
{	
	if ( ( (get_wall_time_micros() - interrupt_timestamp) / 1e6) <= 1 ) {
		std::cout << std::endl << "Double Ctrl-C pressed, exiting now!" << std::endl;
		exit(-4);
	} else {
		interrupt_timestamp = get_wall_time_micros();
	}
    if (!gracefully_exit) {
    	std::cout << std::endl;
    	std::cout << "****************************************************************************************" << std::endl;
    	std::cout << "**  The crafting of plots will stop after the creation and copy of the current plot.  **" << std::endl;
    	std::cout << "**         !! If you want to force quit now, press Ctrl-C twice in series !!          **" << std::endl;
    	std::cout << "****************************************************************************************" << std::endl;
    	gracefully_exit = true;
    }
}

static void AddDataToLog(std::string logfile,std::string plotterName,phase4::output_t& out_4)
{
	std::string path=logfile+".plots";
	std::ofstream log(path.data(), std::ios_base::app | std::ios_base::out);
	std::time_t result = std::time(nullptr);

	//timestamp
	log << std::put_time(std::gmtime(&result), "%c %Z") << '\n';
	log << "phase1:" << out_4.phase1_time_in_seconds << "\n";
	log << "phase2:" << out_4.phase2_time_in_seconds << "\n";
	log << "phase3:" << out_4.phase3_time_in_seconds << "\n";
	log << "phase4:" << out_4.phase4_time_in_seconds << "\n";
	log << "total_plot_time:" << out_4.total_plot_time_seconds << "\n";
	log << "plotsize:" << out_4.plot_size << "\n";
	log << "plotname:" << out_4.params.plot_name << "\n";
}

static void AddStringToLog(std::string logfile,std::string logLine)
{
	std::ofstream log(logfile.data(), std::ios_base::app | std::ios_base::out);
	std::time_t result = std::time(nullptr);

	//timestamp and data
	log << std::put_time(std::gmtime(&result), "%c %Z") << ' : '<< logLine << '\n';
}

/*
Use a plotter_name so that if multiple plotters are using the same target we do not get a race condition where
the same file is trying to be written to
*/
static bool CheckWritePermissions(std::string destpath,std::string plotter_name,std::string filename)
{
	const std::string path = destpath + plotter_name+ filename;
	if(auto file = fopen(path.c_str(), "wb")) {
		fclose(file);
		remove(path.c_str());
	} else {
		return false;
	}
	
	return true;	
}
static long CalculateGigabytes(long bytes)
{
	const int CONVERSION_VALUE = 1024;
	double gigabytes;
	
	if(bytes<=0)
	{
		return 0;
	}
	
	gigabytes=bytes/pow(CONVERSION_VALUE, 3);
	
	return (long)gigabytes;	
}

static long GetAvailableFolderBytes(std::string folder)
{
	struct statvfs stat;
	//lets check space available for next target
	if (statvfs(folder.data(), &stat) != 0) 
	{
		//uh oh
		return 0;
	}	
			
	// the available size is f_bsize * f_bavail		
	return stat.f_bsize * stat.f_bavail;//CalculateGigabytes(stat.f_bsize * stat.f_bavail);		
}

static bool FindNextTargetFolder(std::string plotTargetsFile,long plotSizeInBytes,std::string plotter_name,std::string& nextFolder)
{	
	std::fstream newfile;
	newfile.open(plotTargetsFile.data(),std::ios::in);
	long availableBytes;
	if(newfile.is_open())
	{
		std::string folder;
		while(getline(newfile, folder))
		{ 
			availableBytes=GetAvailableFolderBytes(folder);
			if(plotSizeInBytes <=availableBytes)
			{				
				if(!CheckWritePermissions(folder,plotter_name,".final3"))
				{
					std::cout << "Failed to write to final_dir2 directory: '" << folder << "'" << std::endl;
					return -2;
				}	
		
				nextFolder=folder;
				newfile.close(); //close the file object.
				return true;
			}
		}
		newfile.close(); //close the file object.
	}
	
	return false;
}



static void clean_folder(std::string folderPath)
{
	char const *fp = folderPath.data();
    DIR *theFolder = opendir(fp);
    struct dirent *next_file;
    char filepath[256];
	

    while ( (next_file = readdir(theFolder)) != NULL )
    {
		//dont delete . or ..
		if (0==strcmp(next_file->d_name, ".") || 0==strcmp(next_file->d_name, ".."))
		{
			continue;             
		}		
        // build the path for each file in the folder
        sprintf(filepath, "%s%s", fp, next_file->d_name);
        //remove(filepath);
		std::cout << "cleaning up: "<<filepath << std::endl;
    }
    closedir(theFolder);	
}

inline
phase4::output_t create_plot(	const int num_threads,
								const int log_num_buckets,
								const vector<uint8_t>& pool_key_bytes,
								const vector<uint8_t>& farmer_key_bytes,
								const std::string& tmp_dir,
								const std::string& tmp_dir_2)
{
	const auto total_begin = get_wall_time_micros();

	std::cout << "Process ID: " << GETPID() << std::endl;
	std::cout << "Number of Threads: " << num_threads << std::endl;
	std::cout << "Number of Buckets: 2^" << log_num_buckets
			<< " (" << (1 << log_num_buckets) << ")" << std::endl;
	
	bls::G1Element pool_key;
	bls::G1Element farmer_key;
	try {
		pool_key = bls::G1Element::FromByteVector(pool_key_bytes);
	} catch(std::exception& ex) {
		std::cout << "Invalid poolkey: " << bls::Util::HexStr(pool_key_bytes) << std::endl;
		throw;
	}
	try {
		farmer_key = bls::G1Element::FromByteVector(farmer_key_bytes);
	} catch(std::exception& ex) {
		std::cout << "Invalid farmerkey: " << bls::Util::HexStr(farmer_key_bytes) << std::endl;
		throw;
	}
	std::cout << "Pool Public Key:   " << bls::Util::HexStr(pool_key.Serialize()) << std::endl;
	std::cout << "Farmer Public Key: " << bls::Util::HexStr(farmer_key.Serialize()) << std::endl;
	
	vector<uint8_t> seed(32);
	randombytes_buf(seed.data(), seed.size());
	
	bls::AugSchemeMPL MPL;
	const bls::PrivateKey master_sk = MPL.KeyGen(seed);
	
	bls::PrivateKey local_sk = master_sk;
	for(uint32_t i : {12381, 8444, 3, 0}) {
		local_sk = MPL.DeriveChildSk(local_sk, i);
	}
	const bls::G1Element local_key = local_sk.GetG1Element();
	const bls::G1Element plot_key = local_key + farmer_key;
	
	phase1::input_t params;
	{
		vector<uint8_t> bytes = pool_key.Serialize();
		{
			const auto plot_bytes = plot_key.Serialize();
			bytes.insert(bytes.end(), plot_bytes.begin(), plot_bytes.end());
		}
		bls::Util::Hash256(params.id.data(), bytes.data(), bytes.size());
	}
	const std::string plot_name = "plot-k32-" + get_date_string_ex("%Y-%m-%d-%H-%M")
			+ "-" + bls::Util::HexStr(params.id.data(), params.id.size());
	
	std::cout << "Working Directory:   " << (tmp_dir.empty() ? "$PWD" : tmp_dir) << std::endl;
	std::cout << "Working Directory 2: " << (tmp_dir_2.empty() ? "$PWD" : tmp_dir_2) << std::endl;
	std::cout << "Plot Name: " << plot_name << std::endl;
	
	// memo = bytes(pool_public_key) + bytes(farmer_public_key) + bytes(local_master_sk)
	params.memo.insert(params.memo.end(), pool_key_bytes.begin(), pool_key_bytes.end());
	params.memo.insert(params.memo.end(), farmer_key_bytes.begin(), farmer_key_bytes.end());
	{
		const auto bytes = master_sk.Serialize();
		params.memo.insert(params.memo.end(), bytes.begin(), bytes.end());
	}
	params.plot_name = plot_name;
	
	phase1::output_t out_1;
	phase1::compute(params, out_1, num_threads, log_num_buckets, plot_name, tmp_dir, tmp_dir_2);
	
	phase2::output_t out_2;
	phase2::compute(out_1, out_2, num_threads, log_num_buckets, plot_name, tmp_dir, tmp_dir_2);
	
	phase3::output_t out_3;
	phase3::compute(out_2, out_3, num_threads, log_num_buckets, plot_name, tmp_dir, tmp_dir_2);
	
	phase4::output_t out_4;
	phase4::compute(out_3, out_4, num_threads, log_num_buckets, plot_name, tmp_dir, tmp_dir_2);

	//add these to the out4
	out_4.phase1_time_in_seconds=out_1.time_in_seconds;
	out_4.phase2_time_in_seconds=out_2.time_in_seconds;
	out_4.phase3_time_in_seconds=out_3.time_in_seconds;
	
	const auto time_secs = (get_wall_time_micros() - total_begin) / 1e6;
	out_4.total_plot_time_seconds=time_secs;
	std::cout << "Total plot creation time was "
			<< time_secs << " sec (" << time_secs / 60. << " min)" << std::endl;
	return out_4;
}


int main(int argc, char** argv)
{

	cxxopts::Options options("chia_plot",
		"Multi-threaded pipelined Chia k32 plotter"
#ifdef GIT_COMMIT_HASH
		" - " GIT_COMMIT_HASH
#endif
		"\n\n"
		"For <poolkey> and <farmerkey> see output of `chia keys show`.\n"
		"<tmpdir> needs about 220 GiB space, it will handle about 25% of all writes. (Examples: './', '/mnt/tmp/')\n"
		"<tmpdir2> needs about 110 GiB space and ideally is a RAM drive, it will handle about 75% of all writes.\n"
		"Combined (tmpdir + tmpdir2) peak disk usage is less than 256 GiB.\n"
		"In case of <count> != 1, you may press Ctrl-C for graceful termination after current plot is finished,\n"
		"or double press Ctrl-C to terminate immediately.\n"
	);
	
	std::string pool_key_str;
	std::string farmer_key_str;
	std::string tmp_dir;
	std::string tmp_dir2;
	std::string final_dir;
	std::string final_dir2;
	std::string plot_targets_file;
	std::string log_file_path;
	std::string plottter_name;//what do we call this plotter? default chiaplotter
	bool cleanup_before_plotting;
	int num_plots = 1;
	int num_threads = 4;
	int num_buckets = 256;	
	long space_available=0;
	const long k32_plot_size_in_bytes=108830199808;
	
	
	options.allow_unrecognised_options().add_options()(
		"n, count", "Number of plots to create (default = 1, -1 = infinite)", cxxopts::value<int>(num_plots))(
		"r, threads", "Number of threads (default = 4)", cxxopts::value<int>(num_threads))(
		"u, buckets", "Number of buckets (default = 256)", cxxopts::value<int>(num_buckets))(		
		"t, tmpdir", "Temporary directory, needs ~220 GiB (default = $PWD)", cxxopts::value<std::string>(tmp_dir))(
		"2, tmpdir2", "Temporary directory 2, needs ~110 GiB [RAM] (default = <tmpdir>)", cxxopts::value<std::string>(tmp_dir2))(
		"l, logfile", "Path to log file", cxxopts::value<std::string>(log_file_path))(
		"d, finaldir", "Final directory (default = <tmpdir>)", cxxopts::value<std::string>(final_dir))(
		"3, finaldir2", "Final directory2 (default = <tmpdir>)", cxxopts::value<std::string>(final_dir2))(
		"h, name", "name of this plotter (default = chiaplotter", cxxopts::value<std::string>(plottter_name))(		
		"o, plot_targets_file", "File containing a list of folders(mounted nas) we should plot to", cxxopts::value<std::string>(plot_targets_file))(
		"c, cleanup", "cleanup tempdir and tmpdir2 before starting first plot")(
		"p, poolkey", "Pool Public Key (48 bytes)", cxxopts::value<std::string>(pool_key_str))(
		"f, farmerkey", "Farmer Public Key (48 bytes)", cxxopts::value<std::string>(farmer_key_str))(
		"help", "Print help");
	
	if(argc <= 1) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}
	const auto args = options.parse(argc, argv);
	
	cleanup_before_plotting= args["cleanup"].as<bool>();	
	
	if(args.count("help")) {
		std::cout << options.help({""}) << std::endl;
		return 0;
	}
	if(pool_key_str.empty()) {
		std::cout << "Pool Public Key (48 bytes) needs to be specified via -p <hex>, see `chia keys show`." << std::endl;
		return -2;
	}
	if(farmer_key_str.empty()) {
		std::cout << "Farmer Public Key (48 bytes) needs to be specified via -f <hex>, see `chia keys show`." << std::endl;
		return -2;
	}
	if(tmp_dir.empty()) {
		std::cout << "tmpdir needs to be specified via -t path/" << std::endl;
		return -2;
	}
	if(tmp_dir2.empty()) {
		tmp_dir2 = tmp_dir;
	}
	if(final_dir.empty()) {
		final_dir = tmp_dir;
	}
	
	if(plottter_name.empty()) {
		plottter_name = "chiaplotter";
	}
	
	const auto pool_key = hex_to_bytes(pool_key_str);
	const auto farmer_key = hex_to_bytes(farmer_key_str);
	const int log_num_buckets = num_buckets >= 16 ? int(log2(num_buckets)) : num_buckets;

	if(pool_key.size() != bls::G1Element::SIZE) {
		std::cout << "Invalid poolkey: " << bls::Util::HexStr(pool_key) << ", '" << pool_key_str
			<< "' (needs to be " << bls::G1Element::SIZE << " bytes, see `chia keys show`)" << std::endl;
		return -2;
	}
	if(farmer_key.size() != bls::G1Element::SIZE) {
		std::cout << "Invalid farmerkey: " << bls::Util::HexStr(farmer_key) << ", '" << farmer_key_str
			<< "' (needs to be " << bls::G1Element::SIZE << " bytes, see `chia keys show`)" << std::endl;
		return -2;
	}
	if(!tmp_dir.empty() && tmp_dir.find_last_of("/\\") != tmp_dir.size() - 1) {
		std::cout << "Invalid tmpdir: " << tmp_dir << " (needs trailing '/' or '\\')" << std::endl;
		return -2;
	}
	if(!tmp_dir2.empty() && tmp_dir2.find_last_of("/\\") != tmp_dir2.size() - 1) {
		std::cout << "Invalid tmpdir2: " << tmp_dir2 << " (needs trailing '/' or '\\')" << std::endl;
		return -2;
	}
	if(!final_dir.empty() && final_dir.find_last_of("/\\") != final_dir.size() - 1) {
		std::cout << "Invalid finaldir: " << final_dir << " (needs trailing '/' or '\\')" << std::endl;
		return -2;
	}
	if(num_threads < 1 || num_threads > 1024) {
		std::cout << "Invalid threads parameter: " << num_threads << " (supported: [1..1024])" << std::endl;
		return -2;
	}
	if(log_num_buckets < 4 || log_num_buckets > 16) {
		std::cout << "Invalid buckets parameter: 2^" << log_num_buckets << " (supported: 2^[4..16])" << std::endl;
		return -2;
	}
	
	if(!CheckWritePermissions(tmp_dir,plottter_name,".chia_plot_tmp"))
	{
		std::cout << "Failed to write to tmpdir directory: '" << tmp_dir << "'" << std::endl;
		return -2;
	}
	
	if(!CheckWritePermissions(tmp_dir2,plottter_name,".chia_plot_tmp2"))
	{
		std::cout << "Failed to write to tmpdir2 directory: '" << tmp_dir2 << "'" << std::endl;
		return -2;
	}
	
	if(!CheckWritePermissions(final_dir,plottter_name,".chia_plot_final"))
	{
		std::cout << "Failed to write to finaldir directory: '" << final_dir << "'" << std::endl;
		return -2;
	}	
	
	if(!final_dir2.empty())
	{
		if(!CheckWritePermissions(final_dir2,plottter_name,".chia_plot_fina2"))
		{
			std::cout << "Failed to write to final_dir2 directory: '" << final_dir << "'" << std::endl;
			return -2;
		}			
	}
	
	//are we using a plot target list?
	if(!plot_targets_file.empty())
	{
		//yes
		if(final_dir2.empty() && final_dir.empty())
		{
			std::cout << std::endl << "plot target file only works when you have a final_dir or both a final_dir and final_dir2 " << std::endl;	
			return -2;			
		}
	}
			
	const int num_files_max = (1 << log_num_buckets) + 2 * num_threads + 32;
	
#ifndef _WIN32
	if(false) {
		// try to increase the open file limit
		::rlimit the_limit;
		the_limit.rlim_cur = num_files_max + 10;
		the_limit.rlim_max = num_files_max + 10;
		if(setrlimit(RLIMIT_NOFILE, &the_limit)) {
			std::cout << "Warning: setrlimit() failed!" << std::endl;
		}
	}
#endif
	
	{
		// check that we can open required amount of files
		std::vector<std::pair<FILE*, std::string>> files;
		for(int i = 0; i < num_files_max; ++i) {
			const std::string path = tmp_dir + ".chia_plot_tmp." + std::to_string(i);
			if(auto file = fopen(path.c_str(), "wb")) {
				files.emplace_back(file, path);
			} else {
				std::cout << "Cannot open at least " << num_files_max
						<< " files, please raise maximum open file limit in OS." << std::endl;
				return -2;
			}
		}
		for(const auto& entry : files) {
			fclose(entry.first);
			remove(entry.second.c_str());
		}
	}

	if(num_plots > 1 || num_plots < 0) {
		std::signal(SIGINT, interrupt_handler);
		std::signal(SIGTERM, interrupt_handler);
	}
	
	std::cout << "Multi-threaded pipelined Chia k32 plotter"; 
	#ifdef GIT_COMMIT_HASH
		std::cout << " - " << GIT_COMMIT_HASH;
	#endif	
	std::cout << std::endl;
	std::cout << "Final Directory: " << final_dir << std::endl;
	if(num_plots >= 0) {
		std::cout << "Number of Plots: " << num_plots << std::endl;
	} else {
		std::cout << "Number of Plots: infinite" << std::endl;
	}
	
	Thread<std::tuple<std::string, std::string, std::string, std::string>> copy_thread(
		[](std::tuple<std::string, std::string, std::string, std::string>& from_to) {
			const auto total_begin = get_wall_time_micros();

			//little bit easier to read
			std::tmp_dir=std::get<0>(from_to);
			std::string final_dir1=std::get<1>(from_to);
			std::string final_dir2=std::get<2>(from_to);
			std::string logfile=std::get<3>(from_to)+".copying";	
			double time1=0.0f;		
			double time2=0.0f;	
			int failures=0;	

			//std::string path=logfile+".copying";
			//std::ofstream log(path.data(), std::ios_base::app | std::ios_base::out);
			//std::time_t result = std::time(nullptr);

			//timestamp
			//log << std::put_time(std::gmtime(&result), "%c %Z") << '\n';


			//we always copy from tmp to first location
			while(true) {
				try {
					const auto bytes = final_copy(tmp_dir, final_dir1);
					
					time1 = (get_wall_time_micros() - total_begin) / 1e6;
					std::cout << "Copy to " << final_dir1 << " finished, took " << time << " sec, "
							<< ((bytes / time1) / 1024 / 1024) << " MB/s avg." << std::endl;

					AddStringToLog(logfile,"success: copy to " << final_dir1 << " finished, took " << time << " sec, "
							<< ((bytes / time1) / 1024 / 1024) << " MB/s avg.");
							
					break;
				} catch(const std::exception& ex) {
					std::cout << "Copy to " << final_dir1 << " failed with: " << ex.what() << std::endl;
					std::this_thread::sleep_for(std::chrono::minutes(10));
					if(failures==0)
					{
						AddStringToLog(logfile,"failure: copy to " << final_dir1 << " failed with: " << ex.what());
					}
					++failures;
				}
			}

			//if this isn't empty
			if(!final_dir2.empty())
			{
				//second copy
				const auto total_begin2 = get_wall_time_micros();
				while(true) {
					try {
						const auto bytes = final_copy(final_dir1, final_dir2);
						
						time2 = (get_wall_time_micros() - total_begin2) / 1e6;
						std::cout << "Copy to " << final_dir2 << " finished, took " << time << " sec, "
								<< ((bytes / time2) / 1024 / 1024) << " MB/s avg." << std::endl;

						AddStringToLog(logfile,"success: copy to " << final_dir2 << " finished, took " << time2 << " sec, "
							<< ((bytes / time2) / 1024 / 1024) << " MB/s avg.");								
								
						break;
					} catch(const std::exception& ex) {
						std::cout << "Copy to " << final_dir2 << " failed with: " << ex.what() << std::endl;
						std::this_thread::sleep_for(std::chrono::minutes(10));
						if(failures==0)
						{
							AddStringToLog(logfile,"failure: copy to " << final_dir1 << " failed with: " << ex.what());
						}						
						++failures;
					}
				}	
			}
			
		}, "final/copy");
		
		if(cleanup_before_plotting)
		{
			clean_folder(tmp_dir);
			clean_folder(tmp_dir2);
		}
	
		for(int i = 0; i < num_plots || num_plots < 0; ++i)
		{
			bool plot=false;
			space_available=0;
			
			if (gracefully_exit) {
				std::cout << std::endl << "Process has been interrupted, waiting for copy/rename operations to finish ..." << std::endl;
				break;
			}			
			
			/*are we using a plot target list
				if using a plot list final_dir or final_dir2 just needs to have
				a value when the plotter is executed so 
				
				-d yes
				-3 yes
			*/
			if(!plot_targets_file.empty())
			{
				//find us a home
				std::string newTarget;
				if(FindNextTargetFolder(plot_targets_file,k32_plot_size_in_bytes,plottter_name,newTarget))
				{
					std::cout << std::endl << "final destination set: "<< final_dir2 << std::endl;					
				}					
				
				//are we using two final folders?
				if(!final_dir2.empty())
				{
					//yes
					final_dir2 =newTarget;
					space_available=GetAvailableFolderBytes(final_dir2);
					
				}
				else if(final_dir.empty())
				{
					//nope just one
					final_dir =newTarget;
					space_available=GetAvailableFolderBytes(final_dir2);
				}
				
				std::cout << std::endl << "final destination changed to "<< newTarget << std::endl;
			}
			
			//can we fit a plot in there?
			plot=(space_available < k32_plot_size_in_bytes);
			
			if(plot)
			{
				std::cout << "Crafting plot " << i+1 << " out of " << num_plots << std::endl;	
				const auto out = create_plot(num_threads, log_num_buckets, pool_key, farmer_key, tmp_dir, tmp_dir2);

				if(final_dir != tmp_dir)
				{
					const auto dst_path = final_dir + out.params.plot_name + ".plot";
					const auto dst_path2 = final_dir2 + out.params.plot_name + ".plot";
					std::cout << "Started copy to " << dst_path << " then to" << dst_path2 << std::endl;					
					copy_thread.take_copy(std::make_tuple(out.plot_file_name, dst_path,dst_path2));
				}					
			}
			else
			{
				std::cout << std::endl << "waiting for more space to become available" << std::endl;
				std::this_thread::sleep_for(std::chrono::minutes(1));
			}
		}
		
		copy_thread.close();
	
	return 0;
}


