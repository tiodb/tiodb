#pragma once
#include "pch.h"
#include <memory.h>
#include "../../submodules/http-parser/http_parser.h"


namespace tio
{
	using std::string;
	using std::map;

	struct HTTP_MESSAGE
	{
		string url;
		map<string, string> headers;
	};

	class HttpParser
	{
		http_parser_settings settings_;
		http_parser parser_;

		string nextHeaderName_;
		HTTP_MESSAGE currentMessage_;


		static int on_header_value(http_parser* parser, const char* str, size_t len)
		{
			static_cast<HttpParser*>(parser->data)->SetNextHeaderValue(str, len);
			return 0;
		}

		static int on_message_begin(http_parser* parser)
		{
			static_cast<HttpParser*>(parser->data)->Clear();
			return 0;
		}

		static int on_url(http_parser* parser, const char* str, size_t len)
		{
			static_cast<HttpParser*>(parser->data)->SetUrl(str, len);
			return 0;
		}

		static int on_header_field(http_parser* parser, const char* str, size_t len)
		{
			static_cast<HttpParser*>(parser->data)->SetNextHeaderName(str, len);
			return 0;
		}

		void Clear()
		{
			currentMessage_ = HTTP_MESSAGE();
			nextHeaderName_.clear();
		}

		void SetNextHeaderName(const char* str, size_t len)
		{
			nextHeaderName_.assign(str, len);
		}

		void SetUrl(const char* str, size_t len)
		{
			currentMessage_.url.assign(str, len);
		}

		void SetNextHeaderValue(const char* str, size_t len)
		{
			currentMessage_.headers[nextHeaderName_].assign(str, len);
		}

	public:
		HttpParser()
		{
			http_parser_init(&parser_, HTTP_REQUEST);
			
			parser_.data = this;

			memset(&settings_, 0, sizeof(settings_));

			settings_.on_message_begin = &HttpParser::on_message_begin;
			settings_.on_header_field = &HttpParser::on_header_field;
			settings_.on_header_value = &HttpParser::on_header_value;
			settings_.on_url = &HttpParser::on_url;
		}

		bool Parse(const char* data, size_t len, HTTP_MESSAGE* httpMessage)
		{
			http_parser_execute(&parser_, &settings_, data, len);
			*httpMessage = currentMessage_;
			return true;
		}


	};
}