package ElasticSearch::Transport::HTTP;

use strict;
use warnings FATAL => 'all';
use LWP::UserAgent();
use LWP::ConnCache();
use HTTP::Request();
use Encode qw(decode_utf8);

use parent 'ElasticSearch::Transport';

#===================================
sub protocol {'http'}
#===================================

#===================================
sub send_request {
#===================================
    my $self   = shift;
    my $server = shift;
    my $params = shift;

    my $method  = $params->{method};
    my $uri     = $self->http_uri( $server, $params->{cmd}, $params->{qs} );
    my $request = HTTP::Request->new( $method, $uri );

    $request->add_content_utf8( $params->{data} )
        if defined $params->{data};

    my $server_response = $self->client->request($request);
    my $content         = $server_response->decoded_content;
    $content = decode_utf8($content) if defined $content;

    return $content if $server_response->is_success;

    my $msg  = $server_response->message;
    my $code = $server_response->code;
    my $type
        = $msg eq 'read timeout' ? 'Timeout'
        : $msg =~ /Can't connect|Server closed connection/ ? 'Connection'
        :                                                    'Request';
    my $error_params = {
        server      => $server,
        status_code => $code,
        status_msg  => $msg,
    };

    if ( $type eq 'Request' ) {
        $error_params->{content} = $content;
    }
    $self->throw( $type, $msg . ' (' . $code . ')', $error_params );
}

#===================================
sub client {
#===================================
    my $self = shift;
    unless ( $self->{_client}{$$} ) {
        $self->{_client} = {
            $$ => LWP::UserAgent->new(
                timeout    => $self->timeout,
                conn_cache => LWP::ConnCache->new
            )
        };

    }
    return $self->{_client}{$$};
}


=head1 NAME

ElasticSearch::Transport::HTTP - LWP based HTTP backend

=head1 DESCRIPTION

ElasticSearch::Transport::HTTP uses L<LWP> to talk to ElasticSearch
over HTTP.

It is currently the default backend if no C<transport> is specified, but
consider trying L<ElasticSearch::Transport::HTTPLite> instead - it is
30% faster.


=head1 SYNOPSIS


    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        # transport   => 'http',
        timeout     => '10',
    );

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport>

=item * L<ElasticSearch::Transport::HTTPLite>

=item * L<ElasticSearch::Transport::Thrift>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
