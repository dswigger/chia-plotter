/*
 * phase4.h
 *
 *  Created on: Jun 2, 2021
 *      Author: mad
 */

#ifndef INCLUDE_CHIA_PHASE4_H_
#define INCLUDE_CHIA_PHASE4_H_

#include <chia/phase3.h>


namespace phase4 {

struct output_t {
	float phase1_time_in_seconds;
	float phase2_time_in_seconds;
	float phase3_time_in_seconds;
	float phase4_time_in_seconds;
	float total_plot_time_seconds;
	phase1::input_t params;
	uint64_t plot_size = 0;
	std::string plot_file_name;
};


} // phase4

#endif /* INCLUDE_CHIA_PHASE4_H_ */
