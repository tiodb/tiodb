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
		string method;
		string url;
		map<string, string> headers;
	};

	class HttpParser
	{
		http_parser_settings settings_;
		http_parser parser_;
		bool finished_ = false;

		string nextHeaderName_;
		HTTP_MESSAGE currentMessage_;

		static int on_header_value(http_parser* parser, const char* str, size_t len)
		{
			static_cast<HttpParser*>(parser->data)->SetNextHeaderValue(str, len);
			return 0;
		}

		static int on_message_begin(http_parser* parser)
		{
			static_cast<HttpParser*>(parser->data)->OnMessageBegin();
			return 0;
		}

		static int on_message_complete(http_parser* parser)
		{
			static_cast<HttpParser*>(parser->data)->OnMessageComplete();
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

		void OnMessageBegin()
		{

		}

		void OnMessageComplete()
		{
			switch (parser_.method)
			{
			case HTTP_GET:
				currentMessage_.method = "GET";
				break;
			case HTTP_POST:
				currentMessage_.method = "POST";
				break;
			case HTTP_DELETE:
				currentMessage_.method = "DELETE";
				break;
			case HTTP_PATCH:
				currentMessage_.method = "PATCH";
				break;
			default:
				break;
			}
			
			finished_ = true;
		}

	public:

		void Reset()
		{
			http_parser_init(&parser_, HTTP_REQUEST);

			parser_.data = this;

			memset(&settings_, 0, sizeof(settings_));

			settings_.on_message_begin = &HttpParser::on_message_begin;
			settings_.on_message_complete = &HttpParser::on_message_complete;
			settings_.on_header_field = &HttpParser::on_header_field;
			settings_.on_header_value = &HttpParser::on_header_value;
			settings_.on_url = &HttpParser::on_url;

			currentMessage_ = HTTP_MESSAGE();
			nextHeaderName_.clear();
			finished_ = false;
		}

		HttpParser()
		{
			Reset();
		}

		bool FeedBytes(const char* data, size_t len)
		{
			assert(!finished_);
			http_parser_execute(&parser_, &settings_, data, len);
			return finished_;
		}

		const HTTP_MESSAGE& currentMessage()
		{
			return currentMessage_;
		}

		bool error() const
		{
			return parser_.http_errno != 0;
		}
	};
}